/*
   Copyright (C) 2018  Marco Almeida <marcoafalmeida@gmail.com>

   This file is part of dynamodb-rename.

   dynamodb-rename is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 2 of the License, or
   (at your option) any later version.

   dynamodb-rename is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License along
   with this program; if not, write to the Free Software Foundation, Inc.,
   51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
*/

package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"reflect"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/marcoalmeida/ratelimiter"
)

const (
	src                               = 0
	dst                               = 1
	defaultConnectRetries             = 3
	defaultWriteWorkers               = 5
	defaultReadQps                    = 10
	defaultWriteQps                   = 5
	maxBatchSize                      = 25
	shardEnumerateIntervalSeconds     = 30
	shardWaitForParentIntervalSeconds = 5
)

type appConfig struct {
	table             [2]string
	region            [2]string
	endpoint          [2]string
	dynamo            [2]*dynamodb.DynamoDB
	stream            *dynamodbstreams.DynamoDBStreams
	maxConnectRetries int
	writeWorkers      int
	readQps           int64
	writeQps          int64
	createDst         bool
	enableStream      bool
	verbose           bool
	// used to walk through the lineage of shards to ensure ordering
	// a child shard can only be processed after the parent is done
	completedShards    map[string]bool
	completedShardLock sync.RWMutex
	// keep track of shards for which a goroutine was already spawned
	activeShardProcessors     map[string]bool
	activeShardProcessorsLock sync.RWMutex
}

func parseFlags() *appConfig {
	cfg := &appConfig{
		completedShards:       make(map[string]bool),
		activeShardProcessors: make(map[string]bool),
	}

	flag.StringVar(&cfg.region[src], "src-region", "us-east-1", "AWS region the source lives in")
	flag.StringVar(&cfg.region[dst], "dst-region", "us-east-1", "AWS region the destination lives in")
	flag.StringVar(&cfg.table[src], "src", "", "Name of the source DynamoDB table")
	flag.StringVar(&cfg.table[dst], "dst", "", "Name of the destination DynamoDB table")
	flag.IntVar(
		&cfg.maxConnectRetries,
		"max-retries",
		defaultConnectRetries,
		"Maximum number of retries (with exponential backoff)",
	)
	flag.IntVar(
		&cfg.writeWorkers,
		"write-workers",
		defaultWriteWorkers,
		"Number of concurrent workers writing to the destination table",
	)
	flag.Int64Var(
		&cfg.readQps,
		"read-qps",
		defaultReadQps,
		"Maximum queries per second on read operations",
	)
	flag.Int64Var(
		&cfg.writeQps,
		"write-qps",
		defaultWriteQps,
		"Maximum queries per second on write operations",
	)
	flag.BoolVar(&cfg.createDst, "create-dst", false, "Create the destination table, if it does not exist")
	flag.BoolVar(&cfg.enableStream, "enable-stream", false, "Enable the stream, if disabled")
	flag.BoolVar(&cfg.verbose, "verbose", false, "Print verbose log messages")

	flag.Parse()

	return cfg
}

func checkFlags(app *appConfig) {
	if app.table[src] == "" {
		log.Fatal("The source DynamoDB table is required")
	}

	if app.table[dst] == "" {
		log.Fatal("The destination DynamoDB table is required")
	}
}

func verbose(cfg *appConfig, format string, a ...interface{}) {
	// it's convenient
	format += "\n"
	if cfg.verbose {
		log.Printf(format, a...)
	}
}

// exponential backoff
func backoff(i int, caller string) {
	wait := math.Pow(2, float64(i)) * 100
	log.Printf("%s: backing off for %f milliseconds\n", caller, wait)
	time.Sleep(time.Duration(wait) * time.Millisecond)
}

// make sure both tables exist and have the same schema
func validateTables(cfg *appConfig) error {
	var err error
	output := make([]*dynamodb.DescribeTableOutput, 2)

	for _, t := range []int{src, dst} {
		output[t], err = cfg.dynamo[t].DescribeTable(&dynamodb.DescribeTableInput{
			TableName: aws.String(cfg.table[t]),
		})
		if err != nil {
			// if the destination table does not exist *and* we were asked to create it
			if t == dst && cfg.createDst {
				log.Println("Creating destination table")
				_, err := cfg.dynamo[dst].CreateTable(&dynamodb.CreateTableInput{
					TableName:            aws.String(cfg.table[dst]),
					KeySchema:            output[src].Table.KeySchema,
					AttributeDefinitions: output[src].Table.AttributeDefinitions,
					ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
						ReadCapacityUnits:  output[src].Table.ProvisionedThroughput.ReadCapacityUnits,
						WriteCapacityUnits: output[src].Table.ProvisionedThroughput.WriteCapacityUnits,
					},
				})
				if err != nil {
					return err
				}

				// wait for the table to be created
				status := ""
				for status != "ACTIVE" {
					response, _ := cfg.dynamo[t].DescribeTable(&dynamodb.DescribeTableInput{
						TableName: aws.String(cfg.table[dst]),
					})
					status = *response.Table.TableStatus
					log.Println("Waiting for destination table to be created")
					time.Sleep(1 * time.Second)
				}

				return err
			}

			return errors.New(fmt.Sprintf("Failed to describe table %s: %s\n", cfg.table[t], err.(awserr.Error)))
		}
	}

	if !(reflect.DeepEqual(output[src].Table.AttributeDefinitions, output[dst].Table.AttributeDefinitions) &&
		reflect.DeepEqual(output[src].Table.KeySchema, output[dst].Table.KeySchema)) {
		msg := fmt.Sprintf(
			"Schema mismatch:\n**%s**\n%v\n**%s**\n%v\n",
			cfg.table[src],
			output[src],
			cfg.table[dst],
			output[dst])
		return errors.New(msg)
	}

	return nil
}

func connect(cfg *appConfig) {
	cfg.dynamo[src] = dynamodb.New(session.Must(
		session.NewSession(
			aws.NewConfig().
				WithRegion(cfg.region[src]).
				WithEndpoint(cfg.endpoint[src]).
				WithMaxRetries(cfg.maxConnectRetries),
		)))
	cfg.dynamo[dst] = dynamodb.New(session.Must(
		session.NewSession(
			aws.NewConfig().
				WithRegion(cfg.region[dst]).
				WithEndpoint(cfg.endpoint[dst]).
				WithMaxRetries(cfg.maxConnectRetries),
		)))
	// streams client -- source table only
	cfg.stream = dynamodbstreams.New(session.Must(
		session.NewSession(
			aws.NewConfig().
				WithRegion(cfg.region[src]).
				WithEndpoint(cfg.endpoint[src]).
				WithMaxRetries(cfg.maxConnectRetries),
		)))
}

func writeBatch(
	batch map[string][]*dynamodb.WriteRequest,
	cfg *appConfig,
) error {
	var err error

	for i := 0; i < cfg.maxConnectRetries; i++ {
		_, err = cfg.dynamo[dst].BatchWriteItem(
			&dynamodb.BatchWriteItemInput{
				RequestItems: batch,
			},
		)
		if err != nil {
			backoff(i, "BatchWrite")
		} else {
			return nil
		}
	}

	return errors.New(fmt.Sprintf(
		"BatchWrite: failed after %d attempts: %s",
		cfg.maxConnectRetries, err.Error(),
	))
}

// receive chunks of items from the channel, as delivered by the reader process that's Scanning the table
// create batch requests of 25 items (max size) and call writeBatch
func writeTable(
	itemsChan <-chan []map[string]*dynamodb.AttributeValue,
	wg *sync.WaitGroup,
	cfg *appConfig,
	id int,
) {
	defer wg.Done()

	rl := ratelimiter.New(cfg.writeQps)
	rl.Debug(cfg.verbose)
	writeRequest := make(map[string][]*dynamodb.WriteRequest, 0)

	for {
		items, more := <-itemsChan
		if !more {
			verbose(cfg, "Write worker %d has finished", id)
			return
		}

		// create groups of 25 items -- max batch size
		for _, item := range items {
			requestSize := len(writeRequest[cfg.table[dst]])
			if (requestSize%maxBatchSize) == 0 && requestSize > 0 {
				rl.Acquire(maxBatchSize)
				err := writeBatch(writeRequest, cfg)
				if err != nil {
					log.Println("Failed to write batch:", err)
				} else {
					verbose(cfg, "Write worker %d successfully wrote %d items", id, requestSize)
				}
				writeRequest = make(map[string][]*dynamodb.WriteRequest, 0)
			} else {
				writeRequest[cfg.table[dst]] = append(writeRequest[cfg.table[dst]], &dynamodb.WriteRequest{
					PutRequest: &dynamodb.PutRequest{
						Item: item,
					}})
			}
		}

		// maybe len(items) % maxBatchSize != 0 and there is still something to process
		requestSize := len(writeRequest[cfg.table[dst]])
		if requestSize > 0 {
			rl.Acquire(maxBatchSize)
			err := writeBatch(writeRequest, cfg)
			if err != nil {
				log.Println("Failed to write batch:", err)
			} else {
				verbose(cfg, "Write worker %d successfully wrote %d items", id, requestSize)
			}
		}
	}
}

func readTable(
	itemsChan chan<- []map[string]*dynamodb.AttributeValue,
	cfg *appConfig,
) error {
	lastEvaluatedKey := make(map[string]*dynamodb.AttributeValue, 0)
	rl := ratelimiter.New(cfg.readQps)
	rl.Debug(cfg.verbose)

	for {
		input := &dynamodb.ScanInput{
			TableName:      aws.String(cfg.table[src]),
			ConsistentRead: aws.Bool(true),
		}
		// include the last key we received (if any) to resume scanning
		if len(lastEvaluatedKey) > 0 {
			input.ExclusiveStartKey = lastEvaluatedKey
		}

		successfulScan := false
		for i := 0; i < cfg.maxConnectRetries; i++ {
			rl.Acquire(1)
			result, err := cfg.dynamo[src].Scan(input)
			if err != nil {
				backoff(i, "Scan")
			} else {
				successfulScan = true
				lastEvaluatedKey = result.LastEvaluatedKey
				// write to the channel
				itemsChan <- result.Items
				verbose(cfg, "Scan: received %d items; last key:\n%v", len(result.Items), lastEvaluatedKey)
				break
			}
		}

		if successfulScan {
			if len(lastEvaluatedKey) == 0 {
				// we're done
				verbose(cfg, "Scan: finished")
				return nil
			}
		} else {
			return errors.New(fmt.Sprintf("Scan: failed after %d attempts\n", cfg.maxConnectRetries))
		}
	}
}

func copyTable(cfg *appConfig) error {
	log.Printf("Copying data from %s to %s...\n", cfg.table[src], cfg.table[dst])
	var wg sync.WaitGroup
	// create a channel to send items from source -> destination
	items := make(chan []map[string]*dynamodb.AttributeValue)
	// launch a pool of workers to write to the destination table (pulling from the channel)
	wg.Add(cfg.writeWorkers)
	for i := 0; i < cfg.writeWorkers; i++ {
		verbose(cfg, "Starting write worker %d", i)
		go writeTable(items, &wg, cfg, i)
	}
	// scan the source table and put each resulting list of items in the channel
	err := readTable(items, cfg)
	// nothing else will be added to the channel
	close(items)
	// if scanning the table failed, there's no point on waiting for the writes to finish
	if err != nil {
		return err
	}
	// wait for everything to be written
	wg.Wait()
	// all good
	log.Println("Finished data copy")
	return nil
}

func currentStreamArn(cfg *appConfig) string {
	input := &dynamodbstreams.ListStreamsInput{TableName: aws.String(cfg.table[src])}

	result, err := cfg.stream.ListStreams(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodbstreams.ErrCodeResourceNotFoundException:
				return ""
			}
		}
	}

	// iterate through all streams, call DescribeStream, and find the one with StreamStatus == ENABLING or ENABLED
	for _, s := range result.Streams {
		input := &dynamodbstreams.DescribeStreamInput{
			StreamArn: aws.String(*s.StreamArn),
			Limit:     aws.Int64(100),
		}
		output, err := cfg.stream.DescribeStream(input)
		if err != nil {
			// might as well just move on to the next one
			continue
		}
		if *output.StreamDescription.StreamStatus == "ENABLED" ||
			*output.StreamDescription.StreamStatus == "ENABLING" {
			return *s.StreamArn
		}
	}

	return ""
}

func getStreamArn(cfg *appConfig) (string, error) {
	arn := currentStreamArn(cfg)

	if arn != "" {
		if cfg.enableStream {
			// make sure we're not asking to a enable a stream when one already exists
			return "", errors.New("DynamoDB stream already enabled: " + arn)
		} else {
			// already enabled, we're good
			verbose(cfg, "Found enabled stream %s", arn)
			return arn, nil
		}
	}

	// enable a stream
	if cfg.enableStream {
		log.Printf("Enabling DynamoDB stream on %s...\n", cfg.table[src])
		input := &dynamodb.UpdateTableInput{
			TableName: aws.String(cfg.table[src]),
			StreamSpecification: &dynamodb.StreamSpecification{
				StreamEnabled:  aws.Bool(true),
				StreamViewType: aws.String(dynamodbstreams.StreamViewTypeNewImage),
			},
		}

		result, err := cfg.dynamo[src].UpdateTable(input)
		if err != nil {
			return "", err
		}

		verbose(cfg, "DynamoDB stream enabled: %s", *result.TableDescription.LatestStreamArn)
		return *result.TableDescription.LatestStreamArn, nil
	}

	return "", errors.New("DynamoDB stream not enabled")
}

func disableStream(cfg *appConfig) error {
	log.Println("Disabling DynamoDB stream...")
	input := &dynamodb.UpdateTableInput{
		TableName: aws.String(cfg.table[src]),
		StreamSpecification: &dynamodb.StreamSpecification{
			StreamEnabled: aws.Bool(false),
		},
	}

	_, err := cfg.dynamo[src].UpdateTable(input)
	if err == nil {
		verbose(cfg, "DynamoDB stream disabled")
	}

	return err
}

func isShardCompleted(shardId *string, cfg *appConfig) bool {
	cfg.completedShardLock.RLock()
	_, ok := cfg.completedShards[*shardId]
	cfg.completedShardLock.RUnlock()

	return ok
}

func markShardCompleted(shardId *string, cfg *appConfig) {
	cfg.completedShardLock.Lock()
	cfg.completedShards[*shardId] = true
	cfg.completedShardLock.Unlock()
}

func insertRecord(item map[string]*dynamodb.AttributeValue, cfg *appConfig) error {
	var err error

	input := &dynamodb.PutItemInput{
		Item:      item,
		TableName: aws.String(cfg.table[dst]),
	}

	for i := 0; i < cfg.maxConnectRetries; i++ {
		_, err = cfg.dynamo[dst].PutItem(input)
		if err == nil {
			return nil
		} else {
			backoff(i, "PutItem")
		}
	}

	return err
}

func removeRecord(key map[string]*dynamodb.AttributeValue, cfg *appConfig) error {
	var err error

	input := &dynamodb.DeleteItemInput{
		Key:       key,
		TableName: aws.String(cfg.table[dst]),
	}

	for i := 0; i < cfg.maxConnectRetries; i++ {
		_, err = cfg.dynamo[dst].DeleteItem(input)
		if err == nil {
			return nil
		} else {
			backoff(i, "DeleteItem")
		}
	}

	return err
}

func writeRecords(records []*dynamodbstreams.Record, cfg *appConfig) {
	var err error

	for _, r := range records {
		err = nil
		switch *r.EventName {
		case "MODIFY":
			fallthrough
		case "INSERT":
			err = insertRecord(r.Dynamodb.NewImage, cfg)
		case "REMOVE":
			err = removeRecord(r.Dynamodb.Keys, cfg)
		default:
			err = errors.New(fmt.Sprintf("Unknown event %s on record %v\n", *r.EventName, r.Dynamodb))
			log.Println(err)
		}

		if err != nil {
			log.Printf("Failed to handle event %s: %s\n", *r.EventName, err)
		} else {
			verbose(cfg, "Handled event %s for record %v\n", *r.EventName, r.Dynamodb)
		}
	}
}

func replayShard(shard *dynamodbstreams.Shard, streamArn string, cfg *appConfig) {
	var err error
	var iterator *dynamodbstreams.GetShardIteratorOutput
	var records *dynamodbstreams.GetRecordsOutput

	// do not start until the parent is done
	if shard.ParentShardId != nil {
		for !isShardCompleted(shard.ParentShardId, cfg) {
			verbose(cfg, "Shard %s waiting for parent %s to complete", *shard.ShardId, *shard.ParentShardId)
			time.Sleep(time.Duration(shardWaitForParentIntervalSeconds) * time.Second)
		}
	}

	shardIteratorInput := &dynamodbstreams.GetShardIteratorInput{
		ShardId:           shard.ShardId,
		ShardIteratorType: aws.String("TRIM_HORIZON"),
		StreamArn:         aws.String(streamArn),
	}
	for i := 0; i < cfg.maxConnectRetries; i++ {
		iterator, err = cfg.stream.GetShardIterator(shardIteratorInput)
		if err != nil {
			if i == (cfg.maxConnectRetries - 1) {
				log.Printf("GetShardIterator error: shard %s: %s\n", *shard.ShardId, err)
				return
			} else {
				backoff(i, "GetShardIterator")
			}
		}

	}

	shardIterator := iterator.ShardIterator
	// when nil, the shard has been closed and the requested iterator will not return any more data
	for shardIterator != nil {
		for i := 0; i < cfg.maxConnectRetries; i++ {
			records, err = cfg.stream.GetRecords(&dynamodbstreams.GetRecordsInput{ShardIterator: shardIterator})
			if err != nil {
				if i == (cfg.maxConnectRetries - 1) {
					log.Printf("GetRecords error: shard %s, iterator %s: %s\n", *shard.ShardId,
						*iterator.ShardIterator, err)
					return
				} else {
					backoff(i, "GetRecords")
				}
			}
		}
		// absence of records does not mean we're done with the shard, but
		// might as well avoid a useless function call
		if len(records.Records) > 0 {
			writeRecords(records.Records, cfg)
		}
		shardIterator = records.NextShardIterator
	}

	// we're done
	markShardCompleted(shard.ShardId, cfg)
	verbose(cfg, "Completed shard %s", *shard.ShardId)
}

func replayStream(streamArn string, cfg *appConfig) error {
	log.Println("Starting DynamoDB stream replay...")
	lastEvaluatedShardId := ""
	var err error
	var result *dynamodbstreams.DescribeStreamOutput

	for {
		input := &dynamodbstreams.DescribeStreamInput{
			StreamArn: aws.String(streamArn),
			Limit:     aws.Int64(100),
		}
		if lastEvaluatedShardId != "" {
			input.ExclusiveStartShardId = aws.String(lastEvaluatedShardId)
		}

		for i := 0; i < cfg.maxConnectRetries; i++ {
			result, err = cfg.stream.DescribeStream(input)
			if err != nil {
				if i == (cfg.maxConnectRetries - 1) {
					return err
				} else {
					backoff(i, "DescribeStream")
				}
			}
		}

		// launch one goroutine for each *new* shard
		for _, shard := range result.StreamDescription.Shards {
			cfg.activeShardProcessorsLock.Lock()
			_, ok := cfg.activeShardProcessors[*shard.ShardId]
			if !ok {
				verbose(cfg, "Starting processor for shard %s", *shard.ShardId)
				go replayShard(shard, streamArn, cfg)
				cfg.activeShardProcessors[*shard.ShardId] = true
			} else {
				verbose(cfg, "Shard %s is already being processed", *shard.ShardId)
			}
			cfg.activeShardProcessorsLock.Unlock()
		}

		if result.StreamDescription.LastEvaluatedShardId != nil {
			lastEvaluatedShardId = *result.StreamDescription.LastEvaluatedShardId
		} else {
			// for now, there are no more shards
			// wait a few seconds before checking again -- this API cannot be called more than 10/s
			verbose(cfg, "Sleeping for %d seconds before refreshing the list of shards", shardEnumerateIntervalSeconds)
			lastEvaluatedShardId = ""
			time.Sleep(time.Duration(shardEnumerateIntervalSeconds) * time.Second)
		}
	}

	// we should really never reach this
	return nil
}

func cleanup(cfg *appConfig) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigs
	log.Printf("Received signal: %v", sig)

	if cfg.enableStream {
		err := disableStream(cfg)
		if err != nil {
			log.Printf("Failed to disable stream: %s\n", err)
		}
	}

	log.Println("Bye!")
	os.Exit(0)
}

func main() {
	// parse and check command line arguments
	cfg := parseFlags()
	checkFlags(cfg)

	// create all clients (DynamoDB for source and destination tables + DynamoDBStreams for source)
	// one client instance per table
	connect(cfg)

	// make sure both tables exist and the schemas match
	err := validateTables(cfg)
	if err != nil {
		log.Fatalln("Table validation failed:", err)
	}

	// enable the stream before starting the copy process so that once that's done
	// we can replay all the data inserted/modified in the meantime and continuing
	//
	streamARN, err := getStreamArn(cfg)
	if err != nil {
		log.Fatalln(err)
	}

	// make sure we clean up the best we can when interrupted, namely that the stream is disabled
	log.Println("Preparing signal handler...")
	go cleanup(cfg)

	// copy the data
	err = copyTable(cfg)
	// if copying the data failed, should just stop here
	if err != nil {
		cleanup(cfg)
		log.Fatalln(err)
	}

	// we're done copying the data, it's safe to replay the stream
	replayStream(streamARN, cfg)
}
