import boto3
import json
from botocore.exceptions import ClientError
import datetime
import logging
import sys

# -----------------------------------------------------------------------------------------------------------

def log_config(logfile, mode):
    # -----------------------------------------------------------------------------------------------------------

    FileFormatter = logging.Formatter(
        "%(asctime)s [%(funcName)-12.12s] [%(levelname)-8.8s]  %(message)s")
    ConsoleFormatter = logging.Formatter(
        "[%(asctime)s] : [%(levelname)-8.8s] : %(message)s")

    logging.getLogger('boto3').setLevel(logging.CRITICAL)
    logging.getLogger('botocore').setLevel(logging.CRITICAL)
    logging.getLogger('nose').setLevel(logging.CRITICAL)
    logging.getLogger('s3transfer').setLevel(logging.CRITICAL)
    logging.getLogger('urllib3').setLevel(logging.CRITICAL)

    global logger
    logger = logging.getLogger()
    level = logging.getLevelName(debug_mode)
    logger.setLevel(level)

    if mode == 'console':
        consoleHandler = logging.StreamHandler()
        consoleHandler.setFormatter(ConsoleFormatter)
        logger.addHandler(consoleHandler)
    elif mode == 'file':
        fileHandler = logging.FileHandler(logfile, mode='w')
        fileHandler.setFormatter(FileFormatter)
        logger.addHandler(fileHandler)
    elif mode == 'both':
        consoleHandler = logging.StreamHandler()
        consoleHandler.setFormatter(ConsoleFormatter)
        fileHandler = logging.FileHandler(logfile, mode='w')
        fileHandler.setFormatter(FileFormatter)
        logger.addHandler(fileHandler)
        logger.addHandler(consoleHandler)

    return logger

# -----------------------------------------------------------------------------------------------------------

def exit_program(msg):
    # -----------------------------------------------------------------------------------------------------------
    logger.error(msg)
    logger.error("Exiting...")
    sys.exit()

# -----------------------------------------------------------------------------------------------------------

def get_tasks_by_replication_istance(ri_arn_list):
    # -----------------------------------------------------------------------------------------------------------

    dms_client = boto3.client('dms')
    task_list = []

    for ri_arn in ri_arn_list:
        try:

            logger.debug(
                "Collecting Task details for Replication Instance : {}".format(ri_arn))
            response = dms_client.describe_replication_tasks(Filters= [{'Name': 'replication-instance-arn', 'Values': [ri_arn]}], WithoutSettings = True)
            logger.debug(
                "Task details for Replication Instance : {}".format(response))

            for task in response['ReplicationTasks']:
                if task['Status'] in ["running", "failed"]:
                    task_list.append(task['ReplicationTaskArn'])

        except ClientError as e:
            if e.response['Error']['Code'] in ['ResourceNotFoundFault', 'InvalidParameterValueException']:
                exit_program(
                    "The Replication Instance ARN [{}] not found".format(ri_arn))
            else:
                raise (e)

    logger.debug("List of all Running Tasks ARN : {}".format(task_list))
    return (task_list)

# -----------------------------------------------------------------------------------------------------------

def get_task_metadata(task_arn):
    # -----------------------------------------------------------------------------------------------------------

    dms_client = boto3.client('dms')

    try:
        # Get DMS Replication Instance Name and Replication Task ID
        response = dms_client.describe_replication_tasks(
            Filters = [{'Name': 'replication-task-arn', 'Values': [task_arn]}], WithoutSettings=True
        )

        task_status = response['ReplicationTasks'][0]['Status']
        ri_arn = response['ReplicationTasks'][0]['ReplicationInstanceArn']
        task_id = task_arn.split(':')[6]
        task_name = response['ReplicationTasks'][0]['ReplicationTaskIdentifier']

        response = dms_client.describe_replication_instances(
            Filters=[{'Name': 'replication-instance-arn', 'Values': [ri_arn]}]
        )
        ri_name = response['ReplicationInstances'][0]['ReplicationInstanceIdentifier']

        task_metadata = {
            "TaskID": task_id, "TaskName": task_name, "TaskStatus": task_status, "ReplicationInstName": ri_name
        }

        return (task_metadata)

    except ClientError as e:
        if e.response['Error']['Code'] in ['ResourceNotFoundFault', 'InvalidParameterValueException']:
            exit_program("The Task ARN [{}] not found".format(task_arn))
        else:
            raise (e)

# -----------------------------------------------------------------------------------------------------------

def get_table_validation_status(task_list):
    # -----------------------------------------------------------------------------------------------------------

    dms_client = boto3.client('dms')

    task_table_stat_dict = {}

    for task_arn in task_list:
        try:
            table_stat_list = []
            table_stat_dict = {}

            dms_paginator = dms_client.get_paginator(
                'describe_table_statistics')
            PaginationConfig = {'MaxItems': 20000, 'PageSize': 500}
            page_iterator    = dms_paginator.paginate(ReplicationTaskArn=task_arn, PaginationConfig= PaginationConfig)

            for page in page_iterator:
                table_stat_list.extend(page['TableStatistics'])

            logger.debug("Table Statistics for Task {} : {}".format(
                task_arn, table_stat_list))

        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundFault':
                exit_program("DMS Task not found : ", task_arn)
            if e.response['Error']['Code'] == 'InvalidResourceStateFault':
                exit_program(
                    "DMS Task is in Invalid Resource State to fetch Table Statistics : ", task_arn)
            else:
                exit_program("Unkonwn Error: {}".format(str(e)))

        for item in table_stat_list:
            table_status = item['TableState']
            if table_stat_dict.get(table_status) is None:
                table_stat_dict[table_status] = [item['TableName']]
            else:
                table_stat_dict[table_status].append(item['TableName'])

        for item in table_stat_list:
            validation_status = item['ValidationState']
            if table_stat_dict.get(validation_status) is None:
                table_stat_dict[validation_status] = [item['TableName']]
            else:
                table_stat_dict[validation_status].append(
                    item['TableName'])

        table_stat_dict['ValidationPendingRecords'] = [x['TableName']
                                                        for x in table_stat_list if x['ValidationPendingRecords'] > 0]
        table_stat_dict['ValidationFailedRecords'] = [x['TableName']
                                                        for x in table_stat_list if x['ValidationFailedRecords'] > 0]
        table_stat_dict['ValidationSuspendedRecords'] = [x['TableName']
                                                            for x in table_stat_list if x['ValidationSuspendedRecords'] > 0]

        logger.debug("Tables by Statistics group for Task {} : {}".format(
            task_arn, table_stat_dict))
        task_table_stat_dict[task_arn] = table_stat_dict

    logger.debug("Table grouping for all DMS Tasks : {}".format(
        task_table_stat_dict))
    return (task_table_stat_dict)

# ---------------------------------------------------------------------------

def publish_custom_metrics(all_metrics, cw_namespace):
    # ---------------------------------------------------------------------------

    cw_client = boto3.client('cloudwatch')

    for task_arn in all_metrics.keys():

        task_metadata = get_task_metadata(task_arn)
        task_id = task_metadata['TaskID']
        ri_name = task_metadata['ReplicationInstName']

        table_stat = all_metrics[task_arn]
        metric_data = []

        logger.debug(
            "Initiating Cloudwatch Metric Publish for Task : {}".format(task_arn))

        for stat in table_stat.keys():
            metric_data.append({
                'MetricName': stat, 'Dimensions': [
                    {'Name': 'ReplicationInstanceIdentifier', 'Value': ri_name}, {'Name': 'ReplicationTaskIdentifier',     'Value': task_id}
                ], 'Timestamp': datetime.datetime.utcnow(), 'Value': len(table_stat[stat]), 'Unit': 'Count'
            })

        logger.debug("Cloudwatch Metric Data : {}".format(metric_data))
        max_metrics = 20
        group = 0

        for x in range(0, len(metric_data), max_metrics):
            group += 1

            # slice the metrics into blocks of 20 or just the remaining metrics
            cw_data = metric_data[x:(x + max_metrics)]

            try:
                cw_client.put_metric_data(Namespace=cw_namespace, MetricData= cw_data)
            except:
                exit_program('Pushing metrics to CloudWatch failed: exception : {}'.format(
                    sys.exc_info()[1]))

        logger.debug(
            "Cloudwatch Metric Publish completed for Task : {}".format(task_arn))

# -----------------------------------------------------------------------------------------------------------

def lambda_handler(event, context):
    # -----------------------------------------------------------------------------------------------------------
    try:

        global debug_mode
        debug_mode = 'DEBUG' if event.get('debug', False) else 'INFO'

        EXECUTION_LOG = 'dummy.txt'
        logger = log_config(EXECUTION_LOG, 'console')
        logger.debug('Input Event  : {}'.format(event))

        if event.get('task_arn_list') is not None:
            logger.info("Input Type : List of DMS Task ARNs")
            task_list = event['task_arn_list']

        elif event.get('ri_arn_list') is not None:
            logger.info(
                "Input Type : List of DMS Replication Instance ARNs")
            ri_arn_list = event['ri_arn_list']
            task_list = get_tasks_by_replication_istance(ri_arn_list)

        else:
            exit_program("Invalid Input")

        all_metrics = get_table_validation_status(task_list)
        publish_custom_metrics(all_metrics, 'CustomMetrics/DMS')

    except ClientError as e:
        exit_program("[main] Unkonwn Error: {}".format(str(e)))
