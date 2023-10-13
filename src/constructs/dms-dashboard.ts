import { PythonFunction } from '@aws-cdk/aws-lambda-python-alpha';
import { Aws, CustomResource, Duration } from 'aws-cdk-lib';
import { Rule, RuleTargetInput, Schedule } from 'aws-cdk-lib/aws-events';
import { LambdaFunction } from 'aws-cdk-lib/aws-events-targets';
import { Effect, ManagedPolicy, PolicyDocument, PolicyStatement, Role, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import { Runtime } from 'aws-cdk-lib/aws-lambda';
import { Provider } from 'aws-cdk-lib/custom-resources';
import { Construct } from 'constructs';

export interface DmsDashboardProps {
  readonly collectionInterval?: number;
  readonly replicationInstanceArn: string;
  readonly numTasks?: number;
}

export class DmsDashboard extends Construct {
  constructor(scope: Construct, id: string, props: DmsDashboardProps) {
    super(scope, id);
    let collectionInterval = props.collectionInterval;
    let riArn = props.replicationInstanceArn;
    let numTasks = props.numTasks;

    if (collectionInterval == undefined) {
      collectionInterval = 5;
    }

    if (collectionInterval < 1 || collectionInterval > 60) {
      throw new Error('Must be a valid integer between 1 and 60.');
    }

    if (numTasks == undefined) {
      numTasks = 3;
    }

    if (numTasks < 1 || numTasks > 30) {
      throw new Error('Must be a valid integer between 1 and 30.');
    }

    const rdsAccessPolicy = new PolicyStatement({
      resources: ['*'],
      actions: [
        'dms:DescribeReplicationInstances',
        'dms:DescribeReplicationSubnetGroups',
        'dms:DescribeReplicationTasks',
      ],
      effect: Effect.ALLOW,
    });

    const dmsTaskReadonlyPolicy = new PolicyStatement({
      resources: ['*'],
      actions: ['dms:DescribeTableStatistics'],
      effect: Effect.ALLOW,
    });

    const allowCWMetricWrite = new PolicyStatement({
      resources: ['*'],
      actions: ['cloudwatch:PutMetricData'],
      effect: Effect.ALLOW,
    });

    const inlinePolicies = {
      RDSAccessPolicy: new PolicyDocument({
        statements: [rdsAccessPolicy],
      }),
      DMSTaskReadonlyPolicy: new PolicyDocument({
        statements: [dmsTaskReadonlyPolicy],
      }),
      AllowCWMetricWrite: new PolicyDocument({
        statements: [allowCWMetricWrite],
      }),
    };

    const dmsLambdaMonitorRole = new Role(this, 'DmsLambdaMonitorRole', {
      assumedBy: new ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
        ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaVPCAccessExecutionRole'),
      ],
      inlinePolicies: inlinePolicies,
    });

    const dmsMonFunction = new PythonFunction(this, 'DmsMonFunction', {
      entry: 'src/lambda/dms-monitoring',
      index: 'index.py',
      handler: 'lambda_handler',
      runtime: Runtime.PYTHON_3_11,
      role: dmsLambdaMonitorRole,
      timeout: Duration.seconds(60),
      reservedConcurrentExecutions: 10,
    });

    new Rule(this, 'DMSMetricCollectionSchedule', {
      description: 'DMS Custom Metric Collection Schedule',
      schedule: Schedule.rate(Duration.minutes(collectionInterval)),
      enabled: true,
      targets: [
        new LambdaFunction(dmsMonFunction, {
          event: RuleTargetInput.fromObject({
            ri_arn_list: [riArn],
          }),
        }),
      ],
    });

    const lambdalogtocloudwatch = new PolicyStatement({
      resources: ['*'], // punting for now
      effect: Effect.ALLOW,
      actions: [
        'logs:CreateLogGroup',
        'logs:CreateLogStream',
        'logs:PutLogEvents',
      ],
    });

    const lambdaDescribeDMS = new PolicyStatement({
      resources: ['*'], // punting for now
      effect: Effect.ALLOW,
      actions: [
        'dms:DescribeReplicationInstances',
        'dms:DescribeEndpoints',
        'dms:DescribeReplicationTasks',
      ],
    });

    const lambdaDescribeRDS = new PolicyStatement({
      resources: ['*'], // punting for now
      effect: Effect.ALLOW,
      actions: [
        'rds:DescribeDBInstances',
      ],
    });

    const lambdaCreateCWDashboard = new PolicyStatement({
      resources: ['*'], // punting for now
      effect: Effect.ALLOW,
      actions: [
        'cloudwatch:PutDashboard',
      ],
    });

    const inlineGetDmsConfigPolicies = {
      Lambdalogtocloudwatch: new PolicyDocument({
        statements: [lambdalogtocloudwatch],
      }),
      LambdaDescribeDMS: new PolicyDocument({
        statements: [lambdaDescribeDMS],
      }),
      LambdaDescribeRDS: new PolicyDocument({
        statements: [lambdaDescribeRDS],
      }),
      LambdaCreateCWDashboard: new PolicyDocument({
        statements: [lambdaCreateCWDashboard],
      }),
    };

    const getDmsConfigRole = new Role(this, 'GetDmsConfigRole', {
      assumedBy: new ServicePrincipal('lambda.amazonaws.com'),
      managedPolicies: [
        ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
      ],
      inlinePolicies: inlineGetDmsConfigPolicies,
    });

    const getDmsConfigFunction = new PythonFunction(this, 'getDMSConfigFunction', {
      entry: 'src/lambda/get-dms-config',
      index: 'index.py',
      handler: 'lambda_handler',
      runtime: Runtime.PYTHON_3_11,
      role: getDmsConfigRole,
      timeout: Duration.seconds(600),
      reservedConcurrentExecutions: 10,
      environment: {
        RI_ARN: riArn,
        NUM_TASKS: String(numTasks),
        STACK_NAME: Aws.STACK_NAME,
        REGION: Aws.REGION,
      },
    });

    const customResourceProvider = new Provider(this, 'CRProvider', {
      onEventHandler: getDmsConfigFunction,
    });

    // Create a new custom resource consumer
    new CustomResource(this, 'CustomResourceDmsDashboard', {
      serviceToken: customResourceProvider.serviceToken,
    });
  }
}