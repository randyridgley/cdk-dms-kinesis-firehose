import os
import boto3
from utils.get_dms_config import get_dms_config
from utils.get_dms_replication_status import get_dms_replication_status
from utils.has_dms_changes import has_dms_changes
from utils.wait_for_dms_status import wait_for_dms_status


dms = boto3.client('dms')
cf = boto3.client('cloudformation')


def handler(event, context):
    try:
        stack_name = os.environ.get('STACK_NAME')
        replication_config_arn = get_dms_config(stack_name)
        status = get_dms_replication_status(replication_config_arn)

        if status == 'running':
            if event['RequestType'] == 'Delete' or has_dms_changes(stack_name):
                stop_cmd = {
                    'ReplicationConfigArn': replication_config_arn
                }
                dms.stop_replication(**stop_cmd)
                wait_for_dms_status(replication_config_arn, 'stopped')
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
