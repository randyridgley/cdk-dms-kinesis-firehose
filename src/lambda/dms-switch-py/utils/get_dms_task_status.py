
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
