import json
import time 

def get_change_set(cf, stack_name, change_set_name, changes=None, next_token=None):
    if changes is None:
        changes = []
    response = cf.describe_change_set(
        StackName=stack_name,
        ChangeSetName=change_set_name,
        NextToken=next_token
    )
    change_set = response.get('Changes', [])
    next_changes = changes + change_set
    if 'NextToken' in response:
        return get_change_set(cf, stack_name, change_set_name, next_changes, response['NextToken'])
    else:
        return next_changes
    
def get_dms_config(cf, stack_name):
    resources = list_stack_resources(cf, stack_name, [])
    dms_configs = [res['PhysicalResourceId']
                   for res in resources if res['ResourceType'] == 'AWS::DMS::ReplicationConfig']
    if dms_configs:
        dms_config = dms_configs[0]
        print(dms_config)
        return dms_config
    else:
        return None


def list_stack_resources(cf, stack_name, resources=None, next_token=None):
    if resources is None:
        resources = []
    response = cf.list_stack_resources(
        StackName=stack_name, NextToken=next_token)
    resource_summaries = response.get('StackResourceSummaries', [])
    resources += resource_summaries
    if 'NextToken' in response:
        return list_stack_resources(cf, stack_name, resources, response['NextToken'])
    else:
        return resources

def get_dms_replication_status(dms, replication_config_arn):
    filters = [
        {
            'Name': 'replication-config-arn',
            'Values': [replication_config_arn]
        }
    ]
    response = dms.describe_replications(Filters=filters)
    status = response['Replications'][0]['Status'] if response.get(
        'Replications') else None
    return status


def get_dms_replication_task_status(dms, replication_task_arn):
    filters = [
        {
            'Name': 'replication-task-arn',
            'Values': [replication_task_arn]
        }
    ]
    response = dms.describe_replication_tasks(
        Filters=filters, WithoutSettings=True)
    status = response['ReplicationTasks'][0]['Status'] if response.get(
        'ReplicationTasks') else None
    return status


def get_dms_task(cf, stack_name):
    resources = list_stack_resources(cf, stack_name, [])
    dms_tasks = [res['PhysicalResourceId']
                 for res in resources if res['ResourceType'] == 'AWS::DMS::ReplicationTask']
    if dms_tasks:
        dms_task = dms_tasks[0]
        return dms_task
    else:
        return None


def has_dms_changes(cf, stack_name):
    stacks = cf.describe_stacks(StackName=stack_name)
    print(json.dumps({'stacks': stacks}, default=str))
    change_set_name = stacks['Stacks'][0]['ChangeSetId'] if stacks.get(
        'Stacks') else ''
    changes = get_change_set(cf, stack_name, change_set_name, [])
    dms_changes = any(change.get('ResourceChange', {}).get(
        'ResourceType', '').startswith('AWS::DMS') for change in changes)
    return dms_changes

def wait_for_dms_config_status(dms, replication_config_arn, target_status):
    status = ''
    for _ in range(24):
        status = get_dms_replication_status(dms, replication_config_arn)
        print(f'DMS status: {status}')
        if status == target_status:
            return status
        time.sleep(10)
    raise Exception(f'DMS not {target_status}: {status}')


def wait_for_dms_status(dms, replication_task_arn, target_status):
    status = ''
    for _ in range(24):
        status = get_dms_replication_status(dms, replication_task_arn)
        print(f'DMS status: {status}')
        if status == target_status:
            return status
        time.sleep(10)
    raise Exception(f'DMS not {target_status}: {status}')

