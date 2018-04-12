CAFS-S3Store
============

A store for `cafs` that persists blobs on amazon S3. Open sourced with permission from
innovation.rocks consulting gmbh.

# API

## `s3store(options)`

Options: 
* `s3`: Properly configured S3 instance, see the `aws-sdk` package
* `bucket`: The name of the bucket to use for each operation
* `log`: Custom log function, should behave like `console.log`


# Debugging

Debug-logs are done by the excellent `debug` package. Just set `DEBUG=cafs:s3store` to see debug
logs of `cafs-s3store`.
