import os
import json
import boto3
import utils

dms = boto3.client('dms')
cf = boto3.client('cloudformation')


def handler(event, context):
    replication_task_arn = os.environ.get('DMS_TASK')
    print(json.dumps({'RequestType': event['RequestType']}))
    try:
        if event['RequestType'] == 'Create':
            start_cmd = {
                'ReplicationTaskArn': replication_task_arn,
                'StartReplicationTaskType': 'start-replication'
            }
            dms.start_replication_task(**start_cmd)
            utils.wait_for_dms_status(replication_task_arn, 'running')
            return {
                'PhysicalResourceId': 'post-dms',
                'Status': 'SUCCESS'
            }
        elif event['RequestType'] == 'Update':
            should_unpause = False
            dms_changes = utils.has_dms_changes(
                event['ResourceProperties']['StackName'])
            if dms_changes:
                should_unpause = True
            else:
                status = utils.get_dms_replication_task_status(replication_task_arn)
                print(f'DMS status: {status}')
                if status in ('stopped', 'ready'):
                    should_unpause = True
            if should_unpause:
                start_cmd = {
                    'ReplicationTaskArn': replication_task_arn,
                    'StartReplicationTaskType': 'resume-processing'
                }
                dms.start_replication_task(**start_cmd)
                utils.wait_for_dms_status(replication_task_arn, 'running')
            return {
                'PhysicalResourceId': 'post-dms',
                'Status': 'SUCCESS'
            }
        else:
            print('No operation for', event['RequestType'])
            return {
                'PhysicalResourceId': 'post-dms',
                'Status': 'SUCCESS'
            }
    except Exception as e:
        print('Failed!', str(e))
        return {
            'PhysicalResourceId': 'post-dms',
            'Reason': str(e),
            'Status': 'FAILED'
        }
