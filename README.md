# dynamodb-rename
It's not actually possible to rename a DynamoDB table.

A common workaround is to create a new table and copy the data (it is also possible to 
use [AWS Data Pipeline](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBPipeline.html) 
to export/import data via S3).


`dynamodb-rename` simplifies the task of copying data to a new table by:
* taking a consistent snapshot of the source table using the `Scan` API;
* writing the scanned data to the destination table with the `BatchWriteItem` API;
* leveraging [DynamoDB Streams](https://aws.amazon.com/blogs/database/dynamodb-streams-use-cases-and-design-patterns/)
to replay all data inserted/modified/deleted on the source table during the copy
operation *and* continuously propagating changes in near real time. 

It can also optionally create the destination table and/or enable a stream on 
the source table.  

A client-side [rate limiter](https://github.com/thumbtack/goratelimit) is used
to control (independently) read and write workloads.

## Example
Copy all items in `original-table` to `new-table`, both in `us-west-2`.

Limit reads to 750 queries per second and writes to 1000 queries per
second, with 10 workers processing writes in parallel.


```
dynamodb-rename --src original-table --src-region us-west-2 --dst new-table --dst-region us-west-2 --write-qps 200 
--read-qps 750 --write-workers 5 --create-dst --enable-stream
```
