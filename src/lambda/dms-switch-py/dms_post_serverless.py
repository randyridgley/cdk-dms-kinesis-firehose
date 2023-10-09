import os
import json
import boto3
from utils.wait_for_dms_status import wait_for_dms_status
from utils.has_dms_changes import has_dms_changes
from utils.get_dms_replication_status import get_dms_replication_status

dms = boto3.client('dms')
cf = boto3.client('cloudformation')


def handler(event, context):
    replication_config_arn = os.environ.get('DMS_TASK')
    print(json.dumps({'RequestType': event['RequestType']}))
    try:
        if event['RequestType'] == 'Create':
            start_cmd = {
                'ReplicationConfigArn': replication_config_arn,
                'StartReplicationType': 'start-replication'
            }
            dms.start_replication(**start_cmd)
            wait_for_dms_status(replication_config_arn, 'running')
            return {
                'PhysicalResourceId': 'post-dms',
                'Status': 'SUCCESS'
            }
        elif event['RequestType'] == 'Update':
            should_unpause = False
            dms_changes = has_dms_changes(
                event['ResourceProperties']['StackName'])
            if dms_changes:
                should_unpause = True
            else:
                status = get_dms_replication_status(replication_config_arn)
                print(f'DMS status: {status}')
                if status in ('stopped', 'ready'):
                    should_unpause = True
            if should_unpause:
                start_cmd = {
                    'ReplicationConfigArn': replication_config_arn,
                    'StartReplicationType': 'resume-processing'
                }
                dms.start_replication(**start_cmd)
                wait_for_dms_status(replication_config_arn, 'running')
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
