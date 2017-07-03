import json
import time

import boto3

STATE_MACHINE_ARN = ""  # State Machine that you want to run in parallel

sfn_client = boto3.client('stepfunctions')


def parallel_execute(event):
    """ Allows you to execute the same function in parallel in AWS Step Functions"""
    sfn_executions = []
    for sfn_input in range(len(event)):
        sfn_exe = sfn_client.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            input=str(json.dumps(sfn_input)))

        sfn_executions.append(sfn_exe['executionArn'])

    return sfn_executions


def loop(event, context):
    """Gathers the outputs of the functions executed in parallel and returns them as one output in a list"""

    sfn_output = []
    sfn_executions = parallel_execute(event)

    while sfn_executions:
        for exe in sfn_executions:
            sfn_details = sfn_client.describe_execution(executionArn=exe)
            if 'SUCCEEDED' in sfn_details['status']:
                for retry in range(3):  # Just in case AWS is just being slow with returning the outputs
                    try:
                        output_check = sfn_client.describe_execution(executionArn=exe)['output']
                        if 'null' not in output_check:  # Lambda function returns null if there is no output
                            sfn_output.append(json.loads(sfn_details['output']))
                        break
                    except KeyError:
                        time.sleep(1)
                        pass

                sfn_executions.remove(exe)

            if 'RUNNING' in sfn_details['status']:
                pass

            if sfn_details['status'] in 'TIMED_OUT':  # Timed out behavior might need to be adjusted later
                print("{} has timed out.".format(exe))
                sfn_executions.remove(exe)

            if sfn_details['status'] in ('FAILED', 'ABORTED'):
                raise Exception("{} did not succeed.".format(exe))

    if sfn_output:  # Prevents the return of an empty list
        return sfn_output
