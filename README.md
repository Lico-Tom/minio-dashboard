# minio-dashboard
Minio dashboard for fun.

## backend api command
### creat bucket
```bash
curl -X POST -H 'Content-Type: application/json' http://localhost:10005/api/minio/buckets -d '{"bucket_name":"test"}'
```
### delete bucket
```bash
curl -X DELETE -H 'Content-Type: application/json' http://localhost:10005/api/minio/buckets/test
```
### list bucket
```bash
curl -X GET -H 'Content-Type: application/json' http://localhost:10005/api/minio/buckets
```
