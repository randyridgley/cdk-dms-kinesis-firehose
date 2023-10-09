import os
import boto3
from utils.get_dms_task import get_dms_task
from utils.get_dms_task_status import get_dms_task_status
from utils.has_dms_changes import has_dms_changes
from utils.wait_for_dms_status import wait_for_dms_status


dms = boto3.client('dms')
cf = boto3.client('cloudformation')


def handler(event, context):
    try:
        stack_name = os.environ.get('STACK_NAME')
        replication_task_arn = get_dms_task(cf, stack_name)
        status = get_dms_task_status(replication_task_arn)
        if status == 'running':
            if event['RequestType'] == 'Delete' or has_dms_changes(cf, stack_name):
                stop_cmd = {
                    'ReplicationTaskArn': replication_task_arn
                }
                dms.stop_replication_task(**stop_cmd)
                wait_for_dms_status(replication_task_arn, 'stopped')
        return {
            'PhysicalResourceId': 'pre-dms',
            'Status': 'SUCCESS'
        }
    except Exception as e:
        print('Failed!', str(e))
        return {
            'PhysicalResourceId': 'pre-dms',
            'Reason': str(e),
            'Status': 'FAILED'
        }

