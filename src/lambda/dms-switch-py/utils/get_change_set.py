
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
