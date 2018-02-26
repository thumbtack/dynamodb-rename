# dynamodb-rename
Rename a DynamoDB table by copying the contents (in real time)

## Example
Copy all items in `original-table` to `new-table`, both created in `us-west-2`.

Limit reads to 750 queries per second and writes to 75 queries per
second, with 10 workers processing writes in parallel.

```
dynamodb-rename --src original-table --src-region us-west-2 --dst new-table --dst-region us-west-2 --write-qps 75 --read-qps 750 --write-workers 10
```
