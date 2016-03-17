"""
Copyright 2016 Nicholas Christian
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

# Just a note: SQS seems to be not meant to send large amounts of data through it because the number of messages
# per batch are limited to 10 and the maximum size of a batch is 256 KB.

# If you are sending large amounts of data make sure the lambda function settings are reflective of the amount
# of time and resources this needs.

from __future__ import print_function

from gzip import open as g_open
from string import printable
from sys import getsizeof
from urllib import unquote_plus

from boto3 import client

SQS = client('sqs')
S3 = client('s3')

QUEUE_NAME = ""
MESSAGE_RETENTION_PERIOD = ''  # In seconds


def memos(event, context):
    print("Loading Function...")

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = unquote_plus(event['Records'][0]['s3']['object']['key']).decode('utf8')

    S3.download_file(bucket, key, '/tmp/%s' % (key.split('/')[-1]))

    # If queue already exists it will just fetch the url of the queue.
    queue_url = SQS.create_queue(QueueName=QUEUE_NAME,
                                 Attributes={'MessageRetentionPeriod': MESSAGE_RETENTION_PERIOD})['QueueUrl']

    with g_open('/tmp/%s' % (key.split('/')[-1]), 'r+') as f:
        batch_of_mess = []
        for identifier, content in enumerate(f.readlines()):
            # Gets rid of odd unicode characters that SQS does not like and the message would fail to send.
            batch_of_mess.append({'Id': str(identifier),
                                  'MessageBody': ''.join(l for l in content if l in printable)})

            # Maximum size of a batch is 256 KB and/or 10 messages.
            if getsizeof(batch_of_mess) >= 225 or len(batch_of_mess) == 10:

                message = SQS.send_message_batch(QueueUrl=queue_url,
                                                 Entries=batch_of_mess)

                # SQS does not throw up an error if a message fails to send.
                if 'Failed' in message:
                    print(message)

                del batch_of_mess[:]
        # Takes the remainder of the messages and sends them to SQS.
        if batch_of_mess:
            last_message = SQS.send_message_batch(QueueUrl=queue_url,
                                                  Entries=batch_of_mess)
            if 'Failed' in last_message:
                print(last_message)

    print("Done!")
