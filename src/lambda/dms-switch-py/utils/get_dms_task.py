from utils.list_stack_resources import list_stack_resources


def get_dms_task(cf, stack_name):
    resources = list_stack_resources(cf, stack_name, [])
    dms_tasks = [res['PhysicalResourceId']
                 for res in resources if res['ResourceType'] == 'AWS::DMS::ReplicationTask']
    if dms_tasks:
        dms_task = dms_tasks[0]
        return dms_task
    else:
        return None
