from utils.list_stack_resources import list_stack_resources


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
