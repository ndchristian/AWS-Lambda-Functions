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

# Currently in AWS Lambda you can only have one function assigned to an event.
# This AWS Lambda function allows the user to invoke other AWS Lambda functions from one event.

# You need to make false events with the function names you are invoking.
# I used a S3 "folder" with a "subdirectory" for each function.
# It does not matter what false events these are because the data from the event will be passed from this function.

from __future__ import print_function

from json import dumps

from boto3 import client
from botocore import exceptions

LAMBDACLI = client('lambda')

# All function names go into the list below.
# Example would be: FUNCTION_NAMES = ['lambda-python','s3-firehose']
FUNCTION_NAMES = []


def execute_functions(event, context):
    print("Loading Function...")
    try:
        for function in FUNCTION_NAMES:
            LAMBDACLI.invoke(FunctionName=function,
                             Payload=dumps(event))
    except exceptions.ClientError as error:
        print(error)
