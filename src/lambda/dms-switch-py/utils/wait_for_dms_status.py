import time
from utils.get_dms_task_status import get_dms_task_status


def wait_for_dms_status(dms, replication_task_arn, target_status):
    status = ''
    for _ in range(24):
        status = get_dms_task_status(dms, replication_task_arn)
        print(f'DMS status: {status}')
        if status == target_status:
            return status
        time.sleep(10)
    raise Exception(f'DMS not {target_status}: {status}')
