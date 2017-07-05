"""
Copyright 2017 Nicholas Christian
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

import json
import time

import boto3
from botocore.exceptions import ClientError

STATE_MACHINE_ARN = ""  # State Machine that you want to run in parallel

sfn_client = boto3.client('stepfunctions')


def parallel_execute(event):
    """ Allows you to execute the same function in parallel in AWS Step Functions"""

    return [
        sfn_client.start_execution(stateMachineArn=STATE_MACHINE_ARN, input=str(json.dumps(sfn_input)))['executionArn']
        for sfn_input in range(len(event))]


def loop(event, context):
    """Gathers the outputs of the functions executed in parallel and returns them as one output in a list"""

    sfn_output = []
    sfn_executions = parallel_execute(event)

    while sfn_executions:
        for exe in sfn_executions:
            try:
                sfn_details = sfn_client.describe_execution(executionArn=exe)
            except ClientError as PossibleThrottlingError:
                if 'Throttling' in str(PossibleThrottlingError):
                    time.sleep(1)  # Back off for a second if throttling occurs
                    continue
                else:
                    raise PossibleThrottlingError

            if sfn_details['status'] in 'SUCCEEDED':
                for retry in range(3):  # Just in case AWS is just being slow with returning the outputs
                    try:
                        try:
                            execution_details = sfn_client.describe_execution(executionArn=exe)['output']
                        except ClientError as PossibleError:
                            if 'Throttling' in str(PossibleError):
                                time.sleep(1)  # Back off for a second if throttling occurs
                                continue
                            else:
                                raise PossibleError

                        if 'null' not in execution_details:  # Lambda function returns null if there is no output
                            sfn_output.append(json.loads(execution_details))
                        break
                    except KeyError:
                        time.sleep(1)
                        pass

                sfn_executions.remove(exe)

            if sfn_details['status'] in 'RUNNING':
                pass

            if sfn_details['status'] in 'TIMED_OUT':  # Timed out behavior might need to be adjusted later
                print("{} has timed out.".format(exe))
                sfn_executions.remove(exe)

            if sfn_details['status'] in ('FAILED', 'ABORTED'):
                raise Exception("{} did not succeed.".format(exe))

    if sfn_output:  # Prevents the return of an empty list
        return sfn_output
