
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
