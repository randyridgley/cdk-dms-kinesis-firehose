import boto3, json 
from botocore.exceptions import ClientError
import datetime  
import logging, sys, os

def set_logger(logger_level):
    global logger 
    logger = logging.getLogger()
    if logger_level == 'INFO':
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.CRITICAL)
    return logger

def get_error_treatment(bucket_issue_resolution,issue_resolution,expression):
    s3_client = boto3.client('s3')   
    
    ## load from central DB, which is now S3
    global error_treatment
    try:
        results = s3_client.select_object_content(
            Bucket=bucket_issue_resolution,
            Key=issue_resolution,
            Expression=expression,
            ExpressionType='SQL',
            InputSerialization={
                'CSV': {
                    'FileHeaderInfo': 'USE',
                    ##'QuoteEscapeCharacter': 'string',
                    'RecordDelimiter': '\n',
                    'FieldDelimiter': ',',
                    ##'QuoteCharacter': 'string',
                    'AllowQuotedRecordDelimiter': True
                }
            },
            OutputSerialization={
                'JSON': {
                    'RecordDelimiter': '\n'
                }
            }
        )
        
        for result in results['Payload']:
            if 'Records' in result:
                error_treatment = str(result['Records']['Payload'].decode('utf-8'))
                logging.info(error_treatment)
                error_treatment = json.loads(error_treatment)
                logging.info(error_treatment['Log_Filter_Pattern'])
                break
                
        return error_treatment

    except ClientError as e:
        logger.info(format(str(e)))
        sys.exit()

def generate_customer_message (sns_message_parameters, error_type):
    customer_message_head = ''
    customer_message_body = ''
    customer_message_recommendation = ''
    customer_message_foot = ''

    ## create Custom message and change timestamps
    customer_message_head='You are receiving this email because your Amazon CloudWatch Alarm "'+sns_message_parameters['alarm_name']+'" in the '+sns_message_parameters['alarm_region']+' region has entered the '+sns_message_parameters['alarm_state']+' state, because "'+sns_message_parameters['alarm_reason']+'" at '+sns_message_parameters['alarm_timestamp'] +'.\n'
    
    ## add Console link https://us-east-1.console.aws.amazon.com/dms/v2/home?region=us-east-1#replicationInstanceDetails/dms-346-2
    customer_message_head=customer_message_head+'\nView DMS tasks associated with this alarm in the AWS Management Console: \n'+ 'https://'+sns_message_parameters['alarm_region']+'.console.aws.amazon.com/dms/v2/home?region='+sns_message_parameters['alarm_region']+'#replicationInstanceDetails/'+sns_message_parameters['ri_name']+'\n'

    if error_type == 'TRUNCATION':
        ## add log examples
        customer_message_body = '\nLog examples:\n'+json.dumps(sns_message_parameters['logs_examples_truncated'],indent=4)+'\n'
        
        ## add LOB related settings
        for task in sns_message_parameters['tasks']: ## extract task_external_id of each task in question
            customer_message_body = customer_message_body + '\nTask ARN: '+sns_message_parameters['tasks'][task]['task_arn'] +':\n'
            customer_message_body = customer_message_body + 'If LOB is enabled for task: '+str(sns_message_parameters['tasks'][task]['task_settings']['SupportLobs'])+'\n'
            if sns_message_parameters['tasks'][task]['task_settings']['SupportLobs']:                
                customer_message_body = customer_message_body + 'If LimitedSizeLobMode is used: '+str(sns_message_parameters['tasks'][task]['task_settings']['LimitedSizeLobMode'])+'\n'
                customer_message_body = customer_message_body + 'LobMaxSize used: '+str(sns_message_parameters['tasks'][task]['task_settings']['LobMaxSize'])+' KB\n'
            if "lob_setting_in_table_mapping" in sns_message_parameters['tasks'][task]:
                customer_message_body = customer_message_body + 'Individual LOB setting in table mapping rules:\n'+json.dumps(sns_message_parameters['tasks'][task]['lob_setting_in_table_mapping'],indent=4)+'\n'
                if "less_max_lob_in_table_mapping" in sns_message_parameters['tasks'][task]['task_settings'] and sns_message_parameters['tasks'][task]['task_settings']['less_max_lob_in_table_mapping']:
                    customer_message_body = customer_message_body + 'Note: If both task setting and table mapping specified, table mapping overrides tasks setting. Note that the max LOB is smaller than the LobMaxSize in task setting, to which LOB is truncated to.\n'

        ## add recommendations for LOB truncation errors. retrieve from the central database with the error signature. 
        customer_message_recommendation = error_treatment['Resolution'] 

    ## elif error_type == '':
        ## to do for other error cases
    else:
        customer_message_body = '\nLog examples:\n'+json.dumps(sns_message_parameters['logs_examples_truncated'],indent=4)+'\n'
        customer_message_recommendation = ''

    customer_message = customer_message_head + customer_message_body + customer_message_recommendation + customer_message_foot
    
    return customer_message

def send_cust_sns_message (subject, customer_message, sns_topic_data_truncation):
    sns_dms = boto3.resource('sns')
    platform_endpoint = sns_dms.PlatformEndpoint(sns_topic_data_truncation)

    try:
        response = platform_endpoint.publish(
            Message=customer_message,
            Subject=subject
        )
        logger.info(response)
    except ClientError as e:
        logger.info(format(str(e)))
        sys.exit()

def dms_log_filter_by_alarm (alarm_time, ri_name, pattern, time_interval):
    cw_log_client = boto3.client('logs') 

    filter_pattern = ''
    ## lambda does not support 3.10 yet where switch case is implemented in python
    if pattern == 'TRUNCATION':
        ## to do: check SQL Server pattern
        filter_pattern = error_treatment['Log_Filter_Pattern'] ## '?"W:  Truncation" ?truncated ?trimmed'
    ## elif pattern == '':
        ## to do for other error cases
    else:
        ## this should not happen
        filter_pattern = ''
        logger.info('No filter pre-defined')

    try:
        return cw_log_client.filter_log_events(
                logGroupName='dms-tasks-'+ri_name,
                logStreamNamePrefix='dms-task',
                startTime=int((alarm_time - datetime.timedelta(minutes=time_interval)).timestamp())*1000, ## epoch alarm_time_epoch - 5 min
                endTime=int((alarm_time + datetime.timedelta(minutes=time_interval)).timestamp())*1000, ## epoch alarm_time_epoch + 5 min, assuming find log first, than alarm
                filterPattern=filter_pattern,
                limit=100
            )
    except ClientError as e:
        logger.info(format(str(e)))
        sys.exit()

def dms_check_truncation (sns_message_parameters, ri_name, arn_prefix, alarm_log):
    dms_client = boto3.client('dms') 

    tasks_truncated_logs_all = alarm_log['events'] ## array

    try:
        if len(tasks_truncated_logs_all) != 0:
            logger.info(tasks_truncated_logs_all)

            ## add subject to the customize message
            sns_message_parameters['subject'] = sns_message_parameters['alarm_state'] + ': '+sns_message_parameters['alarm_name'] + ' in '+sns_message_parameters['alarm_region']
            logger.info(sns_message_parameters['subject'])

            ## check DMS logs related to LOB truncation
            tasks_truncated = set()
            logs_examples_truncated = {}
            for log in tasks_truncated_logs_all:
                if not log['logStreamName'] in tasks_truncated: 
                    tasks_truncated.add(log['logStreamName'])
                    logs_examples_truncated[log['logStreamName']] = log['message']

            ## record log examples for truncation related
            sns_message_parameters['logs_examples_truncated'] = logs_examples_truncated

            logger.info(tasks_truncated)
            logger.info(logs_examples_truncated)
            
            ## record task info for each tasks identified with LOB truncation
            sns_message_parameters['tasks'] = {}
            
            for task in tasks_truncated: 
                ## to do: construct message for SNS
                ## no table name in MySQL, not sure SQL Server
                ## describe table mapping, task setting for LOB
                task_arn = arn_prefix+':task:'+task.removeprefix('dms-task-')
                task_external_id = task.removeprefix('dms-task-')
                task_truncated_lob_settings = dms_client.describe_replication_tasks(
                    Filters=[
                        {
                            'Name': 'replication-task-arn',
                            'Values': [
                                task_arn
                            ]
                        }
                    ],
                    WithoutSettings=False
                )
                task_settings = json.loads(task_truncated_lob_settings['ReplicationTasks'][0]['ReplicationTaskSettings'])
                lob_max_size_table_mapping = float('inf')
                lob_max_size_task_setting = 0
                
                sns_message_parameters['tasks'][task_external_id] = {}
                
                sns_message_parameters['tasks'][task_external_id]['task_arn'] = task_arn
                sns_message_parameters['tasks'][task_external_id]['task_settings'] = {"SupportLobs": task_settings['TargetMetadata']['SupportLobs']}

                if task_settings['TargetMetadata']['SupportLobs'] == True:   
                    if task_settings['TargetMetadata']['LimitedSizeLobMode'] == True:
                        lob_max_size_task_setting = task_settings['TargetMetadata']['LobMaxSize']

                        ## record LOB truncation related task settings
                        sns_message_parameters['tasks'][task_external_id]['task_settings'].update({"LimitedSizeLobMode": task_settings['TargetMetadata']['LimitedSizeLobMode'],"LobMaxSize":task_settings['TargetMetadata']['LobMaxSize']})
                        logger.info('Task ARN: '+task_arn+' LimitedSizeLobMode: '+str(task_settings['TargetMetadata']['LimitedSizeLobMode']))
                        logger.info('Task ARN: '+task_arn+' LobMaxSize: '+ str(task_settings['TargetMetadata']['LobMaxSize']))

                    elif task_settings['TargetMetadata']['FullLobMode'] == True:
                        logger.info('There should not be LOB truncation under "FullLobMode". Issue a support case if LOB truncation is seen under "FullLobMode".')

                else:
                    logger.info('If there is LOB in tables migrated by '+ task_arn +', trun on LOB support for the task by setting "SupportLobs" to true')
                
                task_table_mapping = json.loads(task_truncated_lob_settings['ReplicationTasks'][0]['TableMappings'])
                task_setting_rules = task_table_mapping['rules'] ## array of rules

                for rule in task_setting_rules:
                    if 'lob-settings' in rule:
                        if 'bulk-max-size' in rule['lob-settings']:
                            if 'mode' not in rule['lob-settings'] or rule['lob-settings']['mode'] == 'limited':
                                lob_max_size_table_mapping = int(rule['lob-settings']['bulk-max-size'])
                                sns_message_parameters['tasks'][task_external_id]['lob_setting_in_table_mapping']=rule
                            else:
                                logger.info('No limited LOB mode specified for individual tables in the task ' + task_arn + '. Check LOB setting in task setting for LOB truncation.')
                    else:
                        logger.info('No lob-settings in this task rule '+rule['rule-id']+' in '+task_arn)
                
                ## if both task setting and table mapping specified, table mapping overrides tasks setting. compare LobMaxSize and bulk-max-size
                if lob_max_size_table_mapping < lob_max_size_task_setting:
                    sns_message_parameters['tasks'][task_external_id]['task_settings'].update({"less_max_lob_in_table_mapping": True})
                    logger.info('If both task setting and table mapping specified, table mapping overrides tasks setting. Note that the max LOB is smaller than the LobMaxSize in task setting.')
                    ## to do: handler    
        else:
            logger.info('No truncation found in log stream '+'dms-tasks-'+ri_name)
            sys.exit()
            ## to do: loose time frame and check again

        return sns_message_parameters

    except ClientError as e:
        logger.info(format(str(e)))
        sys.exit()

def lambda_handler(event, context):
    
    set_logger('INFO')

    ## logger.info(str(context))
    ## logger.info(str(event))

    dms_client = boto3.client('dms') 
    
    try:
        ## get from CFN as environment variable
        logger.info("environment variable: " + os.environ['RiARN'])  
        logger.info("environment variable: " + os.environ['NotificationSNSTopic'])  
        logger.info("environment variable: " + os.environ['S3Bucket'])  
        logger.info("environment variable: " + os.environ['S3Key'])  
        logger.info("environment variable: " + os.environ['RiName'])
        logger.info("environment variable: " + os.environ['AlarmName'])    

        ## get from environment variable
        bucket_issue_resolution = os.environ['S3Bucket']
        issue_resolution = os.environ['S3Key'] 
        sns_topic_data_truncation = os.environ['NotificationSNSTopic']
        ri_arn = os.environ['RiARN']
        ri_name = os.environ['RiName']
        alarm_name = os.environ['AlarmName']

        arn_prefix = ri_arn.rpartition(':')[0].rpartition(':')[0]
        sns_message_parameters = {
            "alarm_name": event['detail']['alarmName'],
            "alarm_reason": event['detail']['state']['reason'],
            "alarm_region": event['region'],
            "alarm_state": event['detail']['state']['value'],
            "alarm_timestamp": event['time'],
            "ri_name":ri_name
        }
        
        alarm_time = datetime.datetime.strptime(event['time'], '%Y-%m-%dT%H:%M:%SZ') ## 'time': '2022-07-22T00:02:28Z', 
        issue_type = ''

        ## different error types have different treatments
        if sns_message_parameters['alarm_name'] == alarm_name: ## CFN creates this alarm
            issue_type = 'TRUNCATION'           
            ## load from central DB, which is now S3
            expression = """SELECT * FROM s3object s where s.Issue_Type='TRUNCATION' LIMIT 1"""
            error_treatment = get_error_treatment(bucket_issue_resolution, issue_resolution, expression)
            logger.info('Got from s3 for this type of error: ')
            logger.info(error_treatment)
            ## log filter for truncation
            alarm_log = dms_log_filter_by_alarm (alarm_time, ri_name, issue_type, 10)
            ## check DMS task settings related to LOB truncation
            sns_message_parameters = dms_check_truncation (sns_message_parameters, ri_name, arn_prefix, alarm_log)
            ## generate customized SNS message for LOB truncation
            customer_message = generate_customer_message (sns_message_parameters, issue_type)
            ## send customize message with LOB truncation info checked above
            send_cust_sns_message (sns_message_parameters['subject'], customer_message, sns_topic_data_truncation)
        ## elif pattern == '':
            ## to do for other error cases
        else:
            ## this should not happen
            logger.info('Alarm name ' + sns_message_parameters['alarm_name'] + 'is modified unexpectedly. Should be '+alarm_name)
            sys.exit()

    except ClientError as e:
        logger.info(format(str(e)))
        sys.exit()
