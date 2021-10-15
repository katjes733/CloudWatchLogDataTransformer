# MIT License
# 
# Copyright (c) 2021 Martin Macecek
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import json, boto3, logging, base64, gzip, os
from io import BytesIO
from datetime import datetime

levels = {
    'critical': logging.CRITICAL,
    'error': logging.ERROR,
    'warn': logging.WARNING,
    'info': logging.INFO,
    'debug': logging.DEBUG
}
logger = logging.getLogger()
try:   
    logger.setLevel(levels.get(os.getenv('LOG_LEVEL', 'info').lower()))
except KeyError as e:
    logger.setLevel(logging.INFO)

def transformLogEvent(logEvent, owner, logGroup, logStream):
    rV = {}
    rV['timestamp'] = datetime.fromtimestamp(logEvent['timestamp']/1000.0).isoformat();
    rV['id'] = logEvent['id'];
    rV['type'] = "CloudWatchLogs";
    rV['@message'] = logEvent['message']
    rV['@owner'] = owner
    rV['@log_group'] = logGroup
    rV['@log_stream'] = logStream
    return rV

def createRecordsFromLogEvents(logEvents, owner, logGroup, logStream):
    rV = []
    for logEvent in logEvents:
        transformedLogEvent = transformLogEvent(logEvent, owner, logGroup, logStream)
        dataBytes = json.dumps(transformedLogEvent).encode("utf-8")
        rV.append({"Data": dataBytes})
    return rV

def putRecordsToFirehoseStream(streamName, records, client, attemptsMade, maxAttempts):
    failedRecords = []
    codes = []
    errMsg = ''
    response = None
    try:
        response = client.put_record_batch(DeliveryStreamName=streamName, Records=records)
    except Exception as e:
        failedRecords = records
        errMsg = str(e)

    if not failedRecords and response and response['FailedPutCount'] > 0:
        for idx, res in enumerate(response['RequestResponses']):            
            if 'ErrorCode' not in res or not res['ErrorCode']:
                continue

            codes.append(res['ErrorCode'])
            failedRecords.append(records[idx])

        errMsg = f"Individual error codes: {','.join(codes)}"

    if len(failedRecords) > 0:
        if attemptsMade + 1 < maxAttempts:
            logger.warn(f"Some records failed while calling PutRecordBatch to Firehose stream, retrying. {errMsg}")
            putRecordsToFirehoseStream(streamName, failedRecords, client, attemptsMade + 1, maxAttempts)
        else:
            msg = f"Could not put records after {str(maxAttempts)} attempts. {errMsg}"
            logger.error(msg)
            raise RuntimeError(msg)

def processRecords(records, client, streamName):
    rV = []
    for r in records:
        data = base64.b64decode(r['data'])
        striodata = BytesIO(data)
        with gzip.GzipFile(fileobj=striodata, mode='r') as f:
            data = json.loads(f.read())

        logger.debug(f"data: {data}")
        if data['messageType'] == 'DATA_MESSAGE':
            firehoseRecords = createRecordsFromLogEvents(data['logEvents'], data['owner'], data['logGroup'], data['logStream'])
            logger.debug(f"firehoseRecords: {firehoseRecords}")
            rV.append(firehoseRecords)
            putRecordsToFirehoseStream(streamName, firehoseRecords, client, attemptsMade=0, maxAttempts=20)

    return rV

def lambda_handler(event, context):
    logger.debug(f"Event: {event}")
    logger.info(f"Start processing event records.")
    region = event['region']
    streamName = event['deliveryStreamArn'].split('/')[1]
    client = boto3.client('firehose', region_name=region)
    records = processRecords(event['records'], client, streamName)
    logger.info(f"Finished processing event records.")    
    logger.debug(f"Records: {records}")    