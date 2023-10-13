import { PythonFunction } from "@aws-cdk/aws-lambda-python-alpha";
import { Duration, RemovalPolicy } from "aws-cdk-lib";
import { Alarm, ComparisonOperator, TreatMissingData, Unit } from "aws-cdk-lib/aws-cloudwatch";
import { Rule } from "aws-cdk-lib/aws-events";
import { LambdaFunction } from "aws-cdk-lib/aws-events-targets";
import { Effect, ManagedPolicy, PolicyDocument, PolicyStatement, Role, ServicePrincipal } from "aws-cdk-lib/aws-iam";
import { Runtime } from "aws-cdk-lib/aws-lambda";
import { LogGroup, MetricFilter, RetentionDays } from "aws-cdk-lib/aws-logs";
import { Bucket } from "aws-cdk-lib/aws-s3";
import { Subscription, SubscriptionProtocol, Topic } from "aws-cdk-lib/aws-sns";
import { Construct } from "constructs";


export interface LobTruncationAlertProps {
  readonly collectionInterval: number;
  readonly riArn: string;
  readonly emailAddress: string;
}

export class LobTruncationAlert extends Construct {
  constructor(scope: Construct, id: string, props: LobTruncationAlertProps) {
    super(scope, id);
    
    const topic = new Topic(this, 'DMSDataTruncationTopic', {});
    const bucket = new Bucket(this, 'IssueResolutionBucket');

    new Subscription(this, 'NotificationSubscription', {
      endpoint: props.emailAddress,
      protocol: SubscriptionProtocol.EMAIL,
      topic: topic,
    });

    const logGroup = new LogGroup(this, 'DmsTasksLogGroup', {
      logGroupName: '/dms/lob/alerting/tasks',
      retention: RetentionDays.INFINITE,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    const metric = new MetricFilter(this, 'DmsLobMetricFilter', {
      filterPattern: {
        logPatternString: '?"W:  Truncation" ?truncated ?trimmed'
      },
      metricName: 'DMSLogDataTruncation',
      metricNamespace: 'CustomMetrics/DMS',
      metricValue: String(props.collectionInterval),
      unit: Unit.COUNT,
      logGroup: logGroup,
    });

    const alarm = new Alarm(this, 'DmsLobAlarm', {
      alarmName: 'DMSLobTruncationAlarm',
      alarmDescription: 'A CloudWatch Alarm that is triggered when a CloudWatch log filter is satisfied by a certain DMS error pattern.',
      metric: metric.metric({
        statistic: 'SampleCount',
        period: Duration.seconds(props.collectionInterval),        
      }),      
      evaluationPeriods: 1,
      threshold: 1,
      comparisonOperator: ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
      treatMissingData: TreatMissingData.NOT_BREACHING
    });


    const cloudWatchLogReadPolicy = new PolicyStatement({
      resources: ['*'], // punting for now
      effect: Effect.ALLOW,
      actions: [
        'logs:FilterLogEvents',
      ],
    });

    const dmsReadonlyPolicy = new PolicyStatement({
      resources: ['*'], // punting for now
      effect: Effect.ALLOW,
      actions: [
        'dms:DescribeReplicationInstances',
        'dms:DescribeReplicationTasks',
      ],
    });

    const inlineDmsLambdaMonitorRole = {
      Lambdalogtocloudwatch: new PolicyDocument({
        statements: [dmsReadonlyPolicy],
      }),
      LambdaDescribeDMS: new PolicyDocument({
        statements: [cloudWatchLogReadPolicy],
      }),
    };

    const dmsLambdaMonitorRole = new Role(this, 'DmsLambdaMonitorRole', {
      assumedBy: new ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
        ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaVPCAccessExecutionRole'),
      ],
      inlinePolicies: inlineDmsLambdaMonitorRole,
    });

    bucket.grantRead(dmsLambdaMonitorRole);
    topic.grantPublish(dmsLambdaMonitorRole);

    const dmsIssueCustNotification = new PythonFunction(this, 'dmsIssueCustNotification', {
      entry: 'src/lambda/dms-issue-notification',
      index: 'index.py',
      handler: 'lambda_handler',
      runtime: Runtime.PYTHON_3_11,
      role: dmsLambdaMonitorRole,
      timeout: Duration.seconds(60),
      reservedConcurrentExecutions: 10,
      environment: {
        NotificationSNSTopic: topic.topicArn,
        RiARN: props.riArn,
        S3Bucket: bucket.bucketArn,
        S3Key: 'Issue_Resolution.csv',
        AlarmName: alarm.alarmArn
      },
    });

    new Rule(this, 'DMSCustAlarmChangeEventsRule', {
      ruleName: 'DMSLobTruncationAlarm',
      description: 'Events Rule for triggering Lambda to check DMS issues when CloudWatch alarm state changes',
      eventPattern: {
        source: ['aws.cloudwatch'],
        detailType: ['CloudWatch Alarm State Change'],
        resources: [alarm.alarmArn],
        detail: {
          state: {
            value: ['Alarm']
          }
        }        
      },
      targets: [new LambdaFunction(dmsIssueCustNotification)],
      enabled: true,     
    });
  }
}  