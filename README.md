# CloudWatchLogDataTransformer
Lambda function code that transforms log data from Cloud Watch through a AWS Kinesis Data Stream and forwards the transformed data to AWS Kinesis Delivery Stream for ingestion to AWS OpenSearch

# Additional information:
1. The name of the AWS Kinesis Delivery Stream must be passed to the function using environment variable "DATA_STREAM_NAME"
1. Log levels can be controlled with an environment variable "LOG_LEVEL" (critical, error, warn, info or debug)
1. Permissions must include basic Lambda execution roles as well as Kinesis Data Stream read and Kinesis Delivery Stream PutRecordBatch
