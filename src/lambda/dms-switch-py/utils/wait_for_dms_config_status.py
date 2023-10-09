import time
from utils.get_dms_replication_status import get_dms_replication_status


def wait_for_dms_status(dms, replication_config_arn, target_status):
    status = ''
    for _ in range(24):
        status = get_dms_replication_status(dms, replication_config_arn)
        print(f'DMS status: {status}')
        if status == target_status:
            return status
        time.sleep(10)
    raise Exception(f'DMS not {target_status}: {status}')
