package main

import (
	"log"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const (
	localRegion   = "local"
	localEndpoint = "http://localhost:8000"
	partitionKey  = "pk"
)

var keySchema = []*dynamodb.KeySchemaElement{
	{
		AttributeName: aws.String(partitionKey),
		KeyType:       aws.String(dynamodb.KeyTypeHash),
	},
}

var attributeDefinitions = []*dynamodb.AttributeDefinition{
	{
		AttributeName: aws.String(partitionKey),
		AttributeType: aws.String("N"),
	},
}

var provisionedThroughput = &dynamodb.ProvisionedThroughput{
	ReadCapacityUnits:  aws.Int64(int64(1000)),
	WriteCapacityUnits: aws.Int64(int64(1000)),
}

func setupTest(cfg *appConfig, t *testing.T) {
	var err error

	// create both tables
	for i, table := range cfg.table {
		t.Log("Creating table", table)
		_, err = cfg.dynamo[i].CreateTable(&dynamodb.CreateTableInput{
			TableName:             aws.String(table),
			KeySchema:             keySchema,
			AttributeDefinitions:  attributeDefinitions,
			ProvisionedThroughput: provisionedThroughput,
		})
		if err != nil {
			t.Fatal("Table already exists:", table)
		}

		status := ""
		for status != "ACTIVE" {
			t.Log("Waiting for table to be created:", table)
			time.Sleep(1000 * time.Millisecond)
			response, err := cfg.dynamo[i].DescribeTable(&dynamodb.DescribeTableInput{TableName: aws.String(table)})
			if err != nil {
				// ignore -- may be caused by issues related to eventual consistency
			}
			status = *response.Table.TableStatus
		}
	}

	// write some initial data to the source table
	for i := 0; i < 500; i++ {
		input := &dynamodb.PutItemInput{
			TableName: aws.String(cfg.table[src]),
			Item: map[string]*dynamodb.AttributeValue{
				partitionKey: {N: aws.String(strconv.FormatInt(int64(i), 10))},
			},
		}
		_, err := cfg.dynamo[src].PutItem(input)
		if err != nil {
			t.Fatal("Failed to insert item:", err)
		}
	}
}

func teardown(cfg *appConfig, t *testing.T) {
	for i, table := range cfg.table {
		t.Log("Tearing down table", table)
		cfg.dynamo[i].DeleteTable(&dynamodb.DeleteTableInput{
			TableName: aws.String(table),
		})

		status := "DELETING"
		for status == "DELETING" {
			t.Log("Waiting for table to be deleted:", table)
			time.Sleep(1000 * time.Millisecond)
			res, err := cfg.dynamo[i].DescribeTable(&dynamodb.DescribeTableInput{TableName: aws.String(table)})
			if err != nil {
				// table has been deleted
				break
			}
			status = *res.Table.TableStatus
		}
	}
}

func sourceInDestination(source int, destination int, cfg *appConfig, t *testing.T) {
	lastEvaluatedKey := make(map[string]*dynamodb.AttributeValue, 0)

	for {
		input := &dynamodb.ScanInput{
			TableName:      aws.String(cfg.table[source]),
			ConsistentRead: aws.Bool(true),
			Limit:          aws.Int64(10),
		}
		// include the last key we received (if any) to resume scanning
		if len(lastEvaluatedKey) > 0 {
			input.ExclusiveStartKey = lastEvaluatedKey
		}

		result, err := cfg.dynamo[source].Scan(input)
		if err != nil {
			t.Fatal("Scan failed:", err)
		} else {
			lastEvaluatedKey = result.LastEvaluatedKey
			for _, item := range result.Items {
				input := &dynamodb.GetItemInput{
					TableName: aws.String(cfg.table[destination]),
					Key:       item,
				}
				output, err := cfg.dynamo[destination].GetItem(input)
				if err != nil {
					t.Fatal(err)
				}

				if len(output.Item) <= 0 {
					t.Fatal("Item not found:", item)
				}
			}
		}

		if len(lastEvaluatedKey) == 0 {
			return
		}
	}
}

func continuousWrite(quit <-chan bool, cfg *appConfig, t *testing.T) {
	i := 500

	for {
		select {
		case <-quit:
			return
		default:
			input := &dynamodb.PutItemInput{
				TableName: aws.String(cfg.table[src]),
				Item: map[string]*dynamodb.AttributeValue{
					partitionKey: {N: aws.String(strconv.FormatInt(int64(i), 10))},
				},
			}

			_, err := cfg.dynamo[src].PutItem(input)
			if err != nil {
				t.Fatal("Failed to insert item:", err)
			}
		}
		time.Sleep(100 * time.Millisecond)
		i++
	}
}

func TestAll(t *testing.T) {
	cfg := &appConfig{
		table:                 [2]string{"srcTable", "dstTable"},
		region:                [2]string{localRegion, localRegion},
		endpoint:              [2]string{localEndpoint, localEndpoint},
		maxConnectRetries:     3,
		writeWorkers:          5,
		readWorkers:           4,
		readQps:               1000,
		writeQps:              1000,
		completedShards:       make(map[string]bool),
		activeShardProcessors: make(map[string]bool),
		enableStream:          true,
	}

	connect(cfg)
	// create the tables and write some random data to the source table
	setupTest(cfg, t)
	// call the main set of functions
	validateTables(cfg)
	streamARN, err := getStreamArn(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// keep a goroutine writing some items in the background; use a channel to signal we want to stop
	quit := make(chan bool)
	go continuousWrite(quit, cfg, t)
	// copy the contents of src -> dst
	err = copyTable(cfg)
	if err != nil {
		log.Println("Failed to copy table:", err)
	}
	// run this one in the background so that we don't block forever
	go replayStream(streamARN, cfg)
	// give it some time so that the background process writes a few more elements
	time.Sleep(5 * time.Second)
	quit <- true
	// wait a little just to make sure the stream has been replayed
	time.Sleep(3 * time.Second)
	// make sure both tables have the same data
	sourceInDestination(src, dst, cfg, t)
	sourceInDestination(dst, src, cfg, t)
	// cleanup
	teardown(cfg, t)
}
