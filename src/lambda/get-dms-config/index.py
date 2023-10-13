import json
import cfnresponse
import logging
import traceback
import boto3
import botocore
import os
import re
from datetime import datetime


logger = logging.getLogger()
logger.setLevel(logging.INFO)
DASHBOARD_H1 = 0
current_y = DASHBOARD_H1
riArn = os.environ['RI_ARN']
numTasks = os.environ['NUM_TASKS']
stackName = os.environ['STACK_NAME']
region = os.environ['REGION']

def generate_ri_metrics(y, ri_id):
    d_ri = '{"widgets": [{"height": 3,"width": 24,"y": '+str(y)+',"x": 0,"type": "text","properties": {"markdown": "# DMS Dashboard for Replication Instance '+ri_id+' \n## Replication Instance Metrics \n- Instance Class Scaling Up (Out): high CPU, low freeable memory and high swap usage (swap usage > 0) \n- Storage Scaling Up (Out): low free storage and high disk queue depth, especially when migrating or replication more data (more network receive or transit throughput)."}},{"height": 6,"width": 6,"y": '+str(y+3)+',"x": 0,"type": "metric","properties": {"view": "timeSeries","stacked": false,"metrics": [[ "AWS/DMS", "CPUUtilization", "ReplicationInstanceIdentifier", "'+ri_id+'" ]],"region": "' + region + '","title": "RI CPU Utilization"}},{"height": 6,"width": 6,"y": '+str(y+3)+',"x": 6,"type": "metric","properties": {"metrics": [[ "AWS/DMS", "FreeableMemory", "ReplicationInstanceIdentifier", "'+ri_id+'" ],[ ".", "SwapUsage", ".", ".", { "yAxis": "right" } ]],"view": "timeSeries","stacked": false,"region": "' + region + '","stat": "Average","period": 300,"title": "RI Memory Utilization"}},{"height": 6,"width": 6,"y": '+str(y+3)+',"x": 18,"type": "metric","properties": {"metrics": [[ "AWS/DMS", "NetworkTransmitThroughput", "ReplicationInstanceIdentifier", "'+ri_id+'" ],[ ".", "NetworkReceiveThroughput", ".", "." ],[ ".", "DiskQueueDepth", ".", ".", { "yAxis": "right" } ]],"view": "timeSeries","stacked": false,"region": "' + region + '","stat": "Average","period": 60,"title": "Migration Workloads"}},{"height": 6,"width": 6,"y": '+str(y+3)+',"x": 12,"type": "metric","properties": {"view": "timeSeries","stacked": false,"region": "' + region + '","stat": "Average","period": 60,"title": "RI Free Storage","metrics": [[ "AWS/DMS", "FreeStorageSpace", "ReplicationInstanceIdentifier", "'+ri_id+'" ]]}},'

    global current_y 
    current_y = y+3+6

    return d_ri

def generate_validation_metrics(y, ri_id): ##\''''+ri_id+'''\'
    q = ['''SELECT SUM(\\"Table completed\\")\n  FROM SCHEMA(\\"CustomMetrics/DMS\\", ReplicationInstanceIdentifier, ReplicationTaskIdentifier)\n WHERE ReplicationInstanceIdentifier = '%s'\n GROUP BY ReplicationInstanceIdentifier''','''SELECT SUM(\\"Validated\\")\n  FROM SCHEMA(\\"CustomMetrics/DMS\\", ReplicationInstanceIdentifier, ReplicationTaskIdentifier)\n WHERE ReplicationInstanceIdentifier = '%s'\n GROUP BY ReplicationInstanceIdentifi''','''SELECT SUM(\\"Table error\\")\n  FROM SCHEMA(\\"CustomMetrics/DMS\\", ReplicationInstanceIdentifier, ReplicationTaskIdentifier)\n WHERE ReplicationInstanceIdentifier = '%s'\n GROUP BY ReplicationInstanceIdentifier''', '''SELECT SUM(\\"Pending records\\")\n  FROM SCHEMA(\\"CustomMetrics/DMS\\", ReplicationInstanceIdentifier, ReplicationTaskIdentifier)\n WHERE ReplicationInstanceIdentifier = '%s'\n GROUP BY ReplicationInstanceIdentifier''','''SELECT SUM(\\"No primary Key\\")\n  FROM SCHEMA(\\"CustomMetrics/DMS\\", ReplicationInstanceIdentifier, ReplicationTaskIdentifier)\n WHERE ReplicationInstanceIdentifier = '%s'\n GROUP BY ReplicationInstanceIdentifier''','''SELECT SUM(\\"Not enabled\\")\n  FROM SCHEMA(\\"CustomMetrics/DMS\\", ReplicationInstanceIdentifier, ReplicationTaskIdentifier)\n WHERE ReplicationInstanceIdentifier = '%s'\n GROUP BY ReplicationInstanceIdentifier''','''SELECT SUM(\\"ValidationSuspendedRecords\\")\n  FROM SCHEMA(\\"CustomMetrics/DMS\\", ReplicationInstanceIdentifier, ReplicationTaskIdentifier)\n WHERE ReplicationInstanceIdentifier = '%s'\n GROUP BY ReplicationInstanceIdentifier''','''SELECT SUM(\\"ValidationFailedRecords\\")\n  FROM SCHEMA(\\"CustomMetrics/DMS\\", ReplicationInstanceIdentifier, ReplicationTaskIdentifier)\n WHERE ReplicationInstanceIdentifier = '%s'\n GROUP BY ReplicationInstanceIdentifier''', '''SELECT SUM(\\"ValidationPendingRecords\\")\n  FROM SCHEMA(\\"CustomMetrics/DMS\\", ReplicationInstanceIdentifier, ReplicationTaskIdentifier)\n WHERE ReplicationInstanceIdentifier = '%s'\n GROUP BY ReplicationInstanceIdentifier''']

    d_val_1 = '''{"type": "text","x": 0,"y": '''+str(y)+''',"width": 15,"height": 2,"properties": {"markdown": "## Table Counts per Validation State\nNumber of tables in varied validation state in the most recent three hours ([CloudWatch Metrics Insights currently allow only 3 hours](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch-metrics-insights-limits.html))."}},{"type": "text","x": 15,"y": '''+str(y)+''',"width": 9,"height": 2,"properties": {"markdown": "## Record Counts per Validation State\nTotal number of records for all tables in varied validation state in the most recent three hours ([CloudWatch Metrics Insights currently allow only 3 hours](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch-metrics-insights-limits.html))."}},'''

    w = [2,2,2,3,3,3,3,3,3]
    x = 0
    titles = ['Tables Completed','Validated','Table error','Pending Records','Validation Missing PK','Validation Not Enabled','ValidationSuspendedRecords','ValidationFailedRecords','ValidationPendingRecords']
    d_val = ''
    for i in range(9):
        d_val = d_val + '''{"height": 3,"width": '''+str(w[i])+''',"y": '''+str(y+3)+''',"x":'''+str(x)+''',"type": "metric","properties": {"metrics": [[ { "expression": "'''+q[i].replace('%s',ri_id,1)+'''", "label": "", "id": "q1", "region": "''' + region + '''" } ]],"view": "singleValue","region": "''' + region + '''","title": "'''+titles[i]+'''","yAxis": {"left": {"label": "Count","showUnits": false}},"stat": "Average","period": 300}},'''
        x = w[i]+x

    d_val = d_val_1+d_val

    global current_y 
    current_y = y+3+3

    return d_val

def generate_task_metrics(y, ri_id, task_external_ids):
    ## ${AWS::Region}, y = 14
    ## Task metric description
    d_desc = '{"height": 8,"width": 24,"y": '+str(y)+',"x": 0,"type": "text","properties": {"markdown": "## Task Metrics \n**CDC Latency Definition**\n- CDC Source Latency: Latency between source and replication instance.\n- CDC Target Latency: Latency between source and target. Thus, CDC Target Latency >= CDC Source Latency\n\n**Identify CDC Latency**\n- CDC Source Latency >> 0 and CDC Source Latency = CDC Target Latency : focus on **source** latency\nCDC Source Latency \n  - [Mitigate CDC Source Latency](https://aws.amazon.com/premiumsupport/knowledge-center/dms-high-source-latency/)\n- CDC Source Latency = 0 and CDC Target Latency >> 0: focus on **target** latency\nCDC Source Latency \n  - Incoming changes spikes together with CDC Target Latency\n  - CDCChangesTargetDisk spikes as CDC changes queued up and are saved to disk\n  - [Mitigate CDC target latency](https://aws.amazon.com/premiumsupport/knowledge-center/dms-high-target-latency/)\n\n**Task Recovery**\n\nTask repeatedly recovers indicates that some issue is not able to self healed and need manual intervention. Situations like source or target database downtime, connectivity issue, etc."}},'
    ## CPU metrics per task 
    d_cpu_1 = '{"height": 6,"width": 6,"y": '+str(y+8)+',"x": 0,"type": "metric","properties": {"metrics": ['
    ## Memory metrics per task
    d_mem_1 = '{"height": 6,"width": 6,"y": '+str(y+8)+',"x": 6,"type": "metric","properties": {"metrics": ['
    ## RecoveryCount metrics per task
    d_rec_1 = '{"height": 6,"width": 6,"y": '+str(y+8)+',"x": 12,"type": "metric","properties": {"metrics": ['
    ## Validation Issue
    d_val_1 = '{"height": 6,"width": 6,"y": '+str(y+8)+',"x": 18,"type": "metric","properties": {"metrics": ['
    ## CDC latency per tasks
    d_lag_1 = '{"height": 6,"width": 24,"y": '+str(y+8+6)+',"x": 0,"type": "metric","properties": {"metrics": ['
    ## CDC latency details for each task
    d_lag_tasks = ''
    d_common_1 = ''
    d_cpu_2 = ''
    d_mem_2 = ''
    d_rec_2 = ''
    d_val_2 = ''
    d_lag_2 = ''
    d_common_3 = '[ ".", "ValidationPendingOverallCount", ".", ".", ".", "." ]'
    d_common_4 = ''
    d_common_5 = '[ ".", "CDCLatencyTarget", ".", ".", ".", "." ]'
    d_common_6 = ''
    d_common_7 = '[ ".", "CDCLatencyTarget", ".", ".", ".", "." ],[ ".", "CDCIncomingChanges", ".", ".", ".", ".", { "yAxis": "right" } ],[ ".", "CDCChangesDiskTarget", ".", ".", ".", ".", { "yAxis": "right" } ],[ ".", "CDCChangesDiskSource", ".", ".", ".", ".", { "yAxis": "right" } ]],"view": "timeSeries","stacked": false,"region": "' + region + '","stat": "Average","period": 60,"title": "CDC Latency '
    for i in range(0,len(task_external_ids)):
        d_lag_tasks = d_lag_tasks + '{"height": 6,"width": 6,"y": '+str(y+8+6+6+int(i/4)*6)+',"x":'+str(i%4*6)+',"type": "metric","properties": {"metrics": ['
        d_lag_tasks = d_lag_tasks + '[ "AWS/DMS", "CDCLatencySource", "ReplicationInstanceIdentifier", "'+ri_id+'","ReplicationTaskIdentifier", "'+task_external_ids[i]+'"],' + d_common_7 + task_external_ids[i]+'"}},'
        if i == 0:
            d_cpu_2 = d_cpu_2 + '[ "AWS/DMS", "CPUUtilization", "ReplicationInstanceIdentifier", "'+ri_id+'","ReplicationTaskIdentifier", "'+task_external_ids[i]+'"],'
            d_mem_2 = d_mem_2 + '[ "AWS/DMS", "MemoryUsageBytes", "ReplicationInstanceIdentifier", "'+ri_id+'","ReplicationTaskIdentifier", "'+task_external_ids[i]+'"],'
            d_rec_2 = d_rec_2 + '[ "AWS/DMS", "RecoveryCount", "ReplicationInstanceIdentifier", "'+ri_id+'","ReplicationTaskIdentifier", "'+task_external_ids[i]+'"],'
            d_val_2 = d_val_2 + '[ "AWS/DMS", "ValidationFailedOverallCount", "ReplicationInstanceIdentifier", "'+ri_id+'","ReplicationTaskIdentifier", "'+task_external_ids[i]+'"],' + d_common_3 + ','
            d_lag_2 = d_lag_2 + '[ "AWS/DMS", "CDCLatencySource", "ReplicationInstanceIdentifier", "'+ri_id+'","ReplicationTaskIdentifier", "'+task_external_ids[i]+'"],' + d_common_5 + ','
        elif i > 0 and i < len(task_external_ids) - 1:
            d_common_1 = d_common_1 + '[ "...", "'+task_external_ids[i]+'" ],'
            d_common_4 = d_common_4 + '[ ".", "ValidationFailedOverallCount", ".", ".", ".", "'+task_external_ids[i]+'" ],'+d_common_3+','
            d_common_6 = d_common_6 + '[ ".", "CDCLatencySource", ".", ".", ".", "'+task_external_ids[i]+'" ],'+d_common_5+','
        else:
            d_common_1 = d_common_1 + '[ "...", "'+task_external_ids[i]+'" ]'
            d_common_4 = d_common_4 + '[ ".", "ValidationFailedOverallCount", ".", ".", ".", "'+task_external_ids[i]+'" ],' + d_common_3
            d_common_6 = d_common_6 + '[ ".", "CDCLatencySource", ".", ".", ".", "'+task_external_ids[i]+'" ],' + d_common_5

    d_common_2 = '],"view": "timeSeries","stacked": false,"region": "' + region + '","stat": "Average","period": 60,"title": "'

    d_cpu_3 = 'CPU Utilization by Tasks"}}'
    d_mem_3 = 'Memory Usage by Tasks"}}'
    d_rec_3 = 'Task RecoveryCount"}}'
    d_val_3 = 'Validation Issue"}}'
    d_lag_3 = 'CDC Latency"}}'

    d_cpu = d_cpu_1 + d_cpu_2 + d_common_1 + d_common_2 + d_cpu_3
    d_mem = d_mem_1 + d_mem_2 + d_common_1 + d_common_2 + d_mem_3
    d_rec = d_rec_1 + d_rec_2 + d_common_1 + d_common_2 + d_rec_3
    d_val = d_val_1 + d_val_2 + d_common_4 + d_common_2 + d_val_3
    d_lag = d_lag_1 + d_lag_2 + d_common_6 + d_common_2 + d_lag_3

    ## input: task list; output dashboard json, current y
    task_metrics_json = d_desc + d_cpu +','+ d_mem +','+ d_rec +','+ d_val + ','+ d_lag + ',' + d_lag_tasks

    global current_y 
    current_y = y+8+6+6+int(i/4)*6+6 ## current y = previous y + CPU & memory & recovery & validation + cdc latency + cdc latency for each task, each row = 6 in height

    return task_metrics_json

def generate_task_logs(y, ri_id):
    ## input: task list; output dashboard json, current y
    ## y = 40
    ## A table of DMS warnings and errors per log component
    d_desc = '{"height": 12,"width": 24,"y": '+str(y)+',"x": 0,"type": "text","properties": {"markdown": "## Task Errors \n\n**Logging Level**\n- T: Trace messages are written to the log.\n- D: Debug messages are written to the log.\n- I: Informational messages are written to the log.\n- W: Warnings are written to the log.\n- E: Error messages are written to the log.\n\n**Logging Components**\n- **FILE_FACTORY** – The file factory manages files used for batch apply and batch load, and manages Amazon S3 endpoints.\n- **METADATA_MANAGER** – The metadata manager manages source and target metadata, partitioning, and table state during replication.\n- **SORTER** – The SORTER receives incoming events from the SOURCE_CAPTURE process. The events are batched in transactions, and passed to the TARGET_APPLY service component. If the SOURCE_CAPTURE process produces events faster than the TARGET_APPLY component can consume them, the SORTER component caches the backlogged events to disk or to a swap file. Cached events are a common cause for running out of storage in replication instances. The SORTER service component manages cached events, gathers CDC statistics, and reports task latency.\n- **SOURCE_CAPTURE** – Ongoing replication (CDC) data is captured from the source database or service, and passed to the SORTER service component.\n- **SOURCE_UNLOAD** – Data is unloaded from the source database or service during Full Load.\n- **TABLES_MANAGER** — The table manager tracks captured tables, manages the order of table migration, and collects table statistics.\n- **TARGET_APPLY** – Data and data definition language (DDL) statements are applied to the target database.\n- **TARGET_LOAD** – Data is loaded into the target database.\n- **TASK_MANAGER** – The task manager manages running tasks, and breaks tasks down into sub-tasks for parallel data processing.\n- **TRANSFORMATION** – Table-mapping transformation events. For more information, see Using table mapping to specify task settings.\n- **VALIDATOR/ VALIDATOR_EXT** – The VALIDATOR service component verifies that data was migrated accurately from the source to the target. For more information, see Data validation. \n\n[More details](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.CustomizingTasks.TaskSettings.Logging.html)"}},'
    d_log = '''{"height": 6, "width": 24,"y": '''+str(y+12)+''',"x": 0,"type": "log","properties": { "query": "SOURCE 'dms-tasks-'''+ri_id+'''' | fields @logStream, @message\n| filter @message like /]E:/\n| filter @message not like /DATA_STRUCTURE/\n| parse @message \\"* [* ]*\\" as timestamp, logComponent, error\n| stats count(*) as countLogComponent by @logStream,logComponent\n| sort @logStream\n","region": "''' + region + '''","stacked": false,"title": "Error by log component: dms-tasks-'''+ri_id+'''","view": "table"}},'''
    task_log_json = d_desc+d_log

    global current_y 
    current_y = y+12+6 ## y=58

    return task_log_json


def logs_by_tasks(y, ri_id, tasks_endponts, target_cluster_ids, ri_ips):
    ## if target is mysql / postgresql, put DMS logs and DB logs together per task
    ## tasks_endponts: {"task_external_id":["endpoint_arn","endpoint_type","server_name","user_name","instance_id_extracted"]}
    ## target_cluster_ids: {"instance_id_extracted":"aurora_cluster_id"}, can be null
    ## endpoint type supported: "mysql", "postgres", "mariadb", "aurora", "aurora-postgresql"
    d_logs = ''
    endpoint_types = ("mysql", "postgres", "mariadb", "aurora", "aurora-postgresql", "kinesis")
    logger.info("tasks_endponts")
    logger.info(tasks_endponts)
    for task in tasks_endponts:
        d_desc = '{"height": 1,"width": 24,"y": '+str(y)+',"x": 0,"type": "text","properties": {"markdown": "## Task '+task+'\n"}},'
        d_task_log = '''{"height": 9,"width": 24,"y": '''+str(y+1)+''',"x": 0,"type": "log","properties": {"query": "SOURCE 'dms-tasks-'''+ri_id+'''' | fields @message\n| filter @logStream like /'''+task+'''/\n| filter @message like /]E:/ or @message like /]W:/\n| filter @message not like /DATA_STRUCTURE/","region": "''' + region + '''","stacked": false,"title": "Error history: '''+task+'''","view": "table"}},'''
        d_db_log = ''
        if tasks_endponts[task][1] in endpoint_types:
            logger.info("task")
            logger.info(task)
            cluster_id = -1
            if tasks_endponts[task][4] != -1 and len(target_cluster_ids) != 0: ## Must be Aurora and found the resource from RDS
                if tasks_endponts[task][4] in target_cluster_ids:
                    cluster_id = target_cluster_ids[tasks_endponts[task][4]] ## Aurora instance endpoint provided to DMS endpoint
                else: cluster_id = tasks_endponts[task][4] ## Aurora cluster endpoint provided to DMS endpoint
            d_db_log = generate_db_logs(y+1+9,tasks_endponts[task][1],tasks_endponts[task][2],tasks_endponts[task][3],tasks_endponts[task][4],cluster_id,ri_ips) ## "server_name","user_name","instance_id_extracted","cluster_id" may be -1
        d_logs = d_logs + d_desc + d_task_log + d_db_log
        y = y+1+9+6

    ## Remove the last , in the JSON
    d_logs = d_logs[:-1]

    global current_y 
    current_y = y

    return d_logs

def generate_db_logs(y,endpoint_type,server_name,user_name,instance_id,cluster_id,ri_ips):
    ## output: dms log per task, if target is rds/aurora mysql/postgresql, output db error log
    ## if connection string in scretes manager, then don't display
    ## check server name by RDS instance endpoint pattern
    p_rds = '([0-9a-zA-Z-]{1,63})(\\.[a-zA-Z0-9]+\\.)([a-z]{2}-(?:north|south|central|east|west)(?:east|west)?-[1-9])(\\.rds\\.amazonaws\\.com)'
    ## check server name by aurora cluster endpoint pattern
    p_aurora = '([0-9a-zA-Z-]{1,63})(\\.cluster\\-[a-zA-Z0-9]+\\.)([a-z]{2}-(?:north|south|central|east|west)(?:east|west)?-[1-9])(\\.rds\\.amazonaws\\.com)'
    db_log = ''
    ## TO DO: RDS/Aurora may not have error log published to CW. EnabledCloudwatchLogsExports:[Error]
    logger.info("cluster_id: ")
    logger.info(cluster_id)
    logger.info("instance_id: ")
    logger.info(instance_id)
    logger.info("user_name: ")
    logger.info(user_name)
    logger.info("ri_ips: ")
    logger.info(ri_ips)
    if cluster_id == -1: ## Not Aurora cluster specified for the DMS target endpoint, or it uses a customer DNS
        if bool(re.match(p_rds,server_name)):
            logger.info('Not Aurora cluster specified for the DMS target endpoint, or it uses a customer DNS')
            if endpoint_type == 'mysql' or endpoint_type == 'mariadb':
                db_log = '''{"height": 6,"width": 24,"y": '''+str(y)+''',"x": 0,"type": "log","properties": {"query": "SOURCE '/aws/rds/instance/'''+instance_id+'''/error' | fields @message\n| filter @logStream like \\"'''+instance_id+'''\\"\n| filter @message like \\"[Error]\\" or @message like \\"[Warning]\\"\n| sort @timestamp desc\n| limit 20","region": "''' + region + '''","stacked": false,"title": "Target Database Error Log","view": "table"}},'''
            elif endpoint_type == 'postgres':
                db_log = '''{"height": 6,"width": 24,"y": '''+str(y)+''',"x": 0,"type": "log","properties": {"query": "SOURCE '/aws/rds/instance/'''+instance_id+'''/postgresql' | fields @message\n| filter @logStream like \\"'''+instance_id+'''\\"\n| parse @message \\"* UTC:*(*):*@*:[*]:*: *\\" as @timestamp_utc, @ip, @port, @db_user, @db, @pid, @severity, @info\n| filter @severity in [\\"ERROR\\",\\"WARNING\\",\\"FATAL\\",\\"PANIC\\"]\n| filter @db_user=\\"'''+user_name+'''\\"\n| filter @ip in [\\"'''+'''\\",\\"'''.join(ri_ips)+'''\\"]\n| sort @timestamp desc\n| display @message\n| limit 20","region": "''' + region + '''","title": "Target Database Error Log","view": "table"}},'''
        else:
            logger.info('The "'+server_name+'" provided to the DMS target endpoint with endpoint type '+endpoint_type+' may be a custom DNS or not an actual Aurora nor RDS database. Cannot access to the error log.')
    else: ## Must be an Aurora cluster. If cluster_id != instance_id, it is aurora's instance ID provided to DMS endpoint
        if endpoint_type == 'aurora':
            ## if it is aurora cluster endpoint specified
            if cluster_id != -1:
                db_log = '''{"height": 6,"width": 24,"y": '''+str(y)+''',"x": 0,"type": "log","properties": {"query": "SOURCE '/aws/rds/cluster/'''+cluster_id+'''/error' | fields @message\n| filter @message like \\"[Error]\\" or @message like \\"[Warning]\\"\n| sort @timestamp desc\n| limit 20","region": "''' + region + '''","stacked": false,"title": "Target Database Error Log","view": "table"}},'''
        elif endpoint_type == 'aurora-postgresql':
            db_log = '''{"height": 6,"width": 24,"y": '''+str(y)+''',"x": 0,"type": "log","properties": {"query": "SOURCE '/aws/rds/cluster/'''+cluster_id+'''/postgresql' | fields @message\n| parse @message \\"* UTC:*(*):*@*:[*]:*: *\\" as @timestamp_utc, @ip, @port, @db_user, @db, @pid, @severity, @info\n| filter @severity in [\\"ERROR\\",\\"WARNING\\",\\"FATAL\\",\\"PANIC\\"]\n| filter @db_user=\\"'''+user_name+'''\\"\n| filter @ip in [\\"'''+'''\\",\\"'''.join(ri_ips)+'''\\"]\n| sort @timestamp desc\n| display @message\n| limit 20","region": "''' + region + '''","title": "Target Database Error Log","view": "table"}},'''
        else:
            logger.info('Currently, only support displaying error logs for "mysql", "postgres", "mariadb", "aurora", "aurora-postgresql"')

    return db_log

def lambda_handler(event, context):
    # boto3.session.Session().get_available_services()
    logger.info(boto3.__version__)
    logger.info(botocore.__version__)
    logger.info('------ event ------')
    logger.info(str(event))
    logger.info('------ context ------')
    logger.info(str(context))
    responseData = {}
    try:
        if event['RequestType'] == 'Delete':
            responseData['CustStatus'] = 'it is Delete'
            logger.info(str(responseData['CustStatus']))
            cfnresponse.send(
                event, context, cfnresponse.SUCCESS, responseData)
        elif event['RequestType'] == 'Update' or event['RequestType'] == 'Create':
            responseData['CustStatus'] = 'update or Create'
            logger.info(str(responseData['CustStatus']))
            logger.info('boto3 setup')
            dms_client = boto3.client('dms')
            rds_client = boto3.client('rds')
            cw_client = boto3.client('cloudwatch')

            # Get RI information
            ri_id = ''
            ri_ips = []
            ri_response = dms_client.describe_replication_instances(
                Filters=[
                    {
                        'Name': 'replication-instance-arn',
                        'Values': [
                            riArn
                        ]
                    }
                ]
            )
            if len(ri_response['ReplicationInstances']) > 0:
                responseData['CustStatus'] = 'Replication instance found'
                logger.info(str(responseData['CustStatus']))
                ri_id = ri_response['ReplicationInstances'][0]['ReplicationInstanceIdentifier']
                ri_ips = ri_response['ReplicationInstances'][0]['ReplicationInstancePublicIpAddresses'] + \
                    ri_response['ReplicationInstances'][0]['ReplicationInstancePrivateIpAddresses']
                if None in ri_ips:
                    ri_ips.remove(None)

            # Get task information for the given RI
            task_response = dms_client.describe_replication_tasks(
                Filters=[
                    {
                        'Name': 'replication-instance-arn',
                        'Values': [
                            riArn
                        ]
                    }
                ],
                MaxRecords=100,
                WithoutSettings=True
            )
            # put target endpoints into set(), loop in set(), for each, put in {"endpoint":"engine_type"}
            # {"task_external_id":["endpoint_arn","endpoint_type","server_name","user_name","instance_id_extracted"]}
            tasks_endponts = {}
            task_external_ids = []  # sorted by date
            target_endpoints = set()  # distinct target endpoint ARNs
            target_endpoint_ids = []  # distinct target endpoint ids, can be null
            target_cluster_ids = {}  # {"instance_id_extracted":"aurora_cluster_id"}, can be null

            if len(task_response['ReplicationTasks']) > 0:
                responseData['CustStatus'] = 'Replication tasks found'
                logger.info(str(responseData['CustStatus']))
                tz = None
                for task in task_response['ReplicationTasks']:
                    if 'ReplicationTaskCreationDate' in task:
                        tz = task['ReplicationTaskCreationDate'].tzinfo
                        break
                # use ReplicationTaskCreationDate to determine the latest task to monitor
                tasks = sorted(task_response['ReplicationTasks'], key=lambda x: x.get(
                    'ReplicationTaskCreationDate', datetime.now(tz)), reverse=True)
                num = 0
                for task in tasks:
                    if num < int(numTasks) and num < len(tasks):
                        task_id = task['ReplicationTaskArn'].split(':')[-1]
                        task_external_ids.append(task_id)
                        tasks_endponts[task_id] = [
                            task['TargetEndpointArn']]
                        target_endpoints.add(task['TargetEndpointArn'])
                        num = num + 1
                    else:
                        break

            endpoint_response = {}
            if len(target_endpoints) > 0:
                endpoint_response = dms_client.describe_endpoints(
                    Filters=[
                        {
                            'Name': 'endpoint-arn',
                            'Values': list(target_endpoints)
                        }
                    ]
                )
            # {"endpoint_arn":["endpoint_type","server_name","user_name","instance_id_extracted"]}
            endpoints = {}
            target_supported = set(
                ["mysql", "postgres", "mariadb", "aurora", "aurora-postgresql", "kinesis"])
            if len(endpoint_response['Endpoints']) > 0:
                responseData['CustStatus'] = 'DMS endpoints found'
                logger.info(str(responseData['CustStatus']))
                for endpoint in endpoint_response['Endpoints']:
                    endpoints[endpoint['EndpointArn']] = [
                        endpoint['EngineName']]
                    if 'ServerName' in endpoint:
                        instance_id_extracted = endpoint['ServerName'].split('.')[
                            0]
                        endpoints[endpoint['EndpointArn']].extend(
                            [endpoint['ServerName'], endpoint['Username'], instance_id_extracted])
                        if endpoint['EngineName'] in target_supported:
                            target_endpoint_ids.append(
                                instance_id_extracted)
                    else:
                        logger.info(
                            'Endpoint' + str(endpoint['EndpointArn']) + ' uses AWS Secrete Manager or IAM role for connection. Skip from printing DB logs.')
                        endpoints[endpoint['EndpointArn']].extend(
                            [-1, -1, -1])
                for task in tasks_endponts:
                    tasks_endponts[task].extend(
                        endpoints[tasks_endponts[task][0]])

            # Get RDS cluster endpoint
            # Customer can select a mysql or postgresql for aurora endpoints, and specify instance endpiont for aurora target endpoints
            if len(target_endpoint_ids) != 0:
                rds_response = rds_client.describe_db_instances(
                    Filters=[
                        {
                            'Name': 'db-instance-id',
                            'Values': target_endpoint_ids
                        }
                    ]
                )
                if len(rds_response['DBInstances']) > 0:
                    responseData['CustStatus'] = 'RDS instances found via instance identifier'
                    logger.info(str(responseData['CustStatus']))
                    for rds in rds_response['DBInstances']:
                        if 'DBClusterIdentifier' in rds:
                            target_cluster_ids[rds['DBInstanceIdentifier']
                                                ] = rds['DBClusterIdentifier']
                else:
                    responseData['CustStatus'] = 'RDS instances not found via server name specified for the DMS endpoint'
                    logger.info(str(responseData['CustStatus']))

                # Customer can also specify cluster endpiont for aurora target endpoints
                rds_response = rds_client.describe_db_instances(
                    Filters=[
                        {
                            'Name': 'db-cluster-id',
                            'Values': target_endpoint_ids
                        }
                    ]
                )
                if len(rds_response['DBInstances']) > 0:
                    responseData['CustStatus'] = 'RDS instances found via cluster identifier'
                    logger.info(str(responseData['CustStatus']))
                    for rds in rds_response['DBInstances']:
                        if 'DBClusterIdentifier' in rds:
                            target_cluster_ids[rds['DBInstanceIdentifier']
                                                ] = rds['DBClusterIdentifier']
                else:
                    responseData['CustStatus'] = 'RDS instances not found via server name specified for the DMS endpoint'
                    logger.info(str(responseData['CustStatus']))

            logger.info(tasks_endponts)
            logger.info(task_external_ids)
            logger.info(target_endpoints)
            logger.info(target_endpoint_ids)
            logger.info(target_cluster_ids)

            if len(tasks_endponts) > 0:
                # Generate RI metrics
                ri_metrics = generate_ri_metrics(current_y, ri_id)
                # Generate validation metrics
                val_metrics = generate_validation_metrics(current_y, ri_id)
                # Generate task metrics for CloudWatch Dashboard JSON
                task_metrics = generate_task_metrics(
                    current_y, ri_id, task_external_ids)
                # Generate a table of DMS warnings and errors per log component
                task_log_recap = generate_task_logs(current_y, ri_id)
                # Generate JSON for displaying DMS warnings and errors, and error logs from target database
                task_db_logs = logs_by_tasks(
                    current_y, ri_id, tasks_endponts, target_cluster_ids, ri_ips)
                dashboard = ri_metrics+val_metrics+task_metrics+task_log_recap+task_db_logs+']}'
                logger.info("Dashboard json")
                logger.info(dashboard)
                dashboard_json = json.dumps(
                    json.loads(dashboard, strict=False))
                logger.info(dashboard_json)

                cw_response = cw_client.put_dashboard(
                    DashboardName='CFN-' +
                    stackName +'-DMS-Dashboard',
                    DashboardBody=dashboard_json
                )
                logger.info(cw_response)
                if len(cw_response['DashboardValidationMessages']) == 0:
                    responseData['CustStatus'] = 'DMS Dashboard created successfully'
                    logger.info(str(responseData['CustStatus']))
                    responseData['DashboardName'] = stackName + \
                        '-DMS Dashboard'
                    cfnresponse.send(
                        event, context, cfnresponse.SUCCESS, responseData)

        else:
            responseData['CustStatus'] = 'else: '+str(event['RequestType'])
            logger.info(str(responseData['CustStatus']))
            cfnresponse.send(
                event, context, cfnresponse.SUCCESS, responseData)
    except Exception:
        logger.info(traceback.print_exc())
        cfnresponse.send(event, context, cfnresponse.FAILED, {})
    finally:
        return
