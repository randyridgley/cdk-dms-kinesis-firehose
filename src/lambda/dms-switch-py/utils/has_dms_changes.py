import json
from utils.get_change_set import get_change_set


def has_dms_changes(cf, stack_name):
    stacks = cf.describe_stacks(StackName=stack_name)
    print(json.dumps({'stacks': stacks}, default=str))
    change_set_name = stacks['Stacks'][0]['ChangeSetId'] if stacks.get(
        'Stacks') else ''
    changes = get_change_set(cf, stack_name, change_set_name, [])
    dms_changes = any(change.get('ResourceChange', {}).get(
        'ResourceType', '').startswith('AWS::DMS') for change in changes)
    return dms_changes
