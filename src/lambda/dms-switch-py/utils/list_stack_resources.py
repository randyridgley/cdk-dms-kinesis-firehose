
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
