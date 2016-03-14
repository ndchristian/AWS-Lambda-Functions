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

from __future__ import print_function

import gzip
import urllib

from boto3 import client

# All files going in should be gzipped. This lambda function will not work otherwise.

# Prefix in the firehose names to identify which firehoses you wish to activate
PREFIX_HOOK = ''

FH = client('firehose')
S3 = client('s3')


def hose_names():
    # Gathers all firehoses with the correct prefix
    firehose_names = []
    for name in FH.list_delivery_streams()['DeliveryStreamNames']:
        stream_name = FH.describe_delivery_stream(
                DeliveryStreamName=name)['DeliveryStreamDescription']['DeliveryStreamName']

        if PREFIX_HOOK in stream_name:
            firehose_names.append(stream_name)

    return firehose_names


def hydrant(event,context):
    print("Loading function...")

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key']).decode('utf8')

    all_records = []
    firehose_names = hose_names()

    S3.download_file(bucket, key, '/tmp/%s' % (key.split('/')[-1]))

    with gzip.open('/tmp/%s' % (key.split('/')[-1]), 'r+') as f:
        for content in f.readlines():
            all_records.append({'Data': content})

            # Firehoses "put_record_batch" only accepts 500 or less entries
            if len(all_records) == 500:
                for steam_name in firehose_names:
                    FH.put_record_batch(DeliveryStreamName=steam_name,
                                        Records=all_records)

                del all_records[:]

    for steam_name in firehose_names:
        FH.put_record_batch(DeliveryStreamName=steam_name,
                            Records=all_records)

    print("Done!")
