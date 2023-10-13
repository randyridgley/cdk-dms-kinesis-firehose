import { App, Aspects, CfnOutput, Stack, StackProps } from 'aws-cdk-lib';
import { InterfaceVpcEndpointAwsService, Vpc } from 'aws-cdk-lib/aws-ec2';
import { EventType } from 'aws-cdk-lib/aws-s3';
import { SnsDestination } from 'aws-cdk-lib/aws-s3-notifications';
import { Topic } from 'aws-cdk-lib/aws-sns';
import { Construct } from 'constructs';
import { KinesisDMSReplicator } from './constructs/dms-replicator';
import { KinesisPipeline } from './constructs/kinesis-pipeline';
import { Notebook } from './constructs/notebook';
import { RDSPostgresDatabase } from './constructs/rds-postgres-db';
import { ArchitectureDiagramAspect } from '@aws-community/arch-dia'
import { LobTruncationAlert } from './constructs/lob-truncation-alert';
import { DmsDashboard } from './constructs/dms-dashboard';
export class DMSKinesisStack extends Stack {
  readonly dmsReplicator: KinesisDMSReplicator;

  constructor(scope: Construct, id: string, props: StackProps = {}) {
    super(scope, id, props);

    const vpc = new Vpc(this, 'ReplicationVPC', {
      maxAzs: 2,
      natGateways: 1,
    });
    vpc.addInterfaceEndpoint('sageMakerNotebookEndpoint', {
      service: InterfaceVpcEndpointAwsService.SAGEMAKER_NOTEBOOK,
    });

    const adminUsername = 'dbAdmin';
    const databaseName = 'employee';

    const db = new RDSPostgresDatabase(this, 'LakeDB', {
      adminUsername: adminUsername,
      databaseName: databaseName,
      vpc: vpc,
    });

    new CfnOutput(this, 'DatabaseHostname', { value: db.database.instanceEndpoint.hostname });

    const notebook = new Notebook(this, 'SageMakerNotebook', {
      vpc: vpc,
      db: db
    });

    new CfnOutput(this, 'Notebook', { value: notebook.notebook.ref });

    const pipeline = new KinesisPipeline(this, 'KinesisPipeline');

    new CfnOutput(this, 'KinesisStream', { value: pipeline.deliveryStream.ref });
    new CfnOutput(this, 'S3Bucket', { value: pipeline.bucket.bucketName });
   
    const topic = new Topic(this, "SNSTopic", {});
    pipeline.bucket.addEventNotification(
      EventType.OBJECT_CREATED,
      new SnsDestination(topic),
    );

    this.dmsReplicator = new KinesisDMSReplicator(this, 'DMSReplicator', {
      vpc: vpc,
      target: {
        stream: pipeline.stream,
        bucket: pipeline.bucket
      },
      source: {
        username: adminUsername,
        sourceDB: db,
        port: db.database.instanceEndpoint.port,
        password: db.adminPassword.secretValueFromJson('password').toString(),
        serverName: db.database.instanceEndpoint.hostname,
        databaseName: databaseName,
      },
      serverless: false,
      taskSettings: this.getTaskSettings(),
    });
  }

  private getTaskSettings(): Object {
    return {
      "TargetMetadata": {
        "TargetSchema": "",
        "SupportLobs": true,
        "FullLobMode": false,
        "LobChunkSize": 0,
        "LimitedSizeLobMode": true,
        "LobMaxSize": 32,
        "InlineLobMaxSize": 0,
        "LoadMaxFileSize": 0,
        "ParallelLoadThreads": 0,
        "ParallelLoadBufferSize": 0,
        "BatchApplyEnabled": false,
        "TaskRecoveryTableEnabled": false,
        "ParallelLoadQueuesPerThread": 0,
        "ParallelApplyThreads": 0,
        "ParallelApplyBufferSize": 0,
        "ParallelApplyQueuesPerThread": 0
      },
      "FullLoadSettings": {
        "TargetTablePrepMode": "DO_NOTHING",
        "CreatePkAfterFullLoad": false,
        "StopTaskCachedChangesApplied": false,
        "StopTaskCachedChangesNotApplied": false,
        "MaxFullLoadSubTasks": 8,
        "TransactionConsistencyTimeout": 600,
        "CommitRate": 10000
      },
      "Logging": {
        "EnableLogging": true,
        "LogComponents": [
          {
            "Id": "DATA_STRUCTURE",
            "Severity": "LOGGER_SEVERITY_DEFAULT"
          },
          {
            "Id": "COMMUNICATION",
            "Severity": "LOGGER_SEVERITY_DEFAULT"
          },
          {
            "Id": "IO",
            "Severity": "LOGGER_SEVERITY_DEFAULT"
          },
          {
            "Id": "COMMON",
            "Severity": "LOGGER_SEVERITY_DEFAULT"
          },
          {
            "Id": "FILE_FACTORY",
            "Severity": "LOGGER_SEVERITY_DEFAULT"
          },
          {
            "Id": "FILE_TRANSFER",
            "Severity": "LOGGER_SEVERITY_DEFAULT"
          },
          {
            "Id": "REST_SERVER",
            "Severity": "LOGGER_SEVERITY_DEFAULT"
          },
          {
            "Id": "ADDONS",
            "Severity": "LOGGER_SEVERITY_DEFAULT"
          },
          {
            "Id": "TARGET_LOAD",
            "Severity": "LOGGER_SEVERITY_DEFAULT"
          },
          {
            "Id": "TARGET_APPLY",
            "Severity": "LOGGER_SEVERITY_DEFAULT"
          },
          {
            "Id": "SOURCE_UNLOAD",
            "Severity": "LOGGER_SEVERITY_DEFAULT"
          },
          {
            "Id": "SOURCE_CAPTURE",
            "Severity": "LOGGER_SEVERITY_DEFAULT"
          },
          {
            "Id": "TRANSFORMATION",
            "Severity": "LOGGER_SEVERITY_DEFAULT"
          },
          {
            "Id": "SORTER",
            "Severity": "LOGGER_SEVERITY_DEFAULT"
          },
          {
            "Id": "TASK_MANAGER",
            "Severity": "LOGGER_SEVERITY_DEFAULT"
          },
          {
            "Id": "TABLES_MANAGER",
            "Severity": "LOGGER_SEVERITY_DEFAULT"
          },
          {
            "Id": "METADATA_MANAGER",
            "Severity": "LOGGER_SEVERITY_DEFAULT"
          },
          {
            "Id": "PERFORMANCE",
            "Severity": "LOGGER_SEVERITY_DEFAULT"
          },
          {
            "Id": "VALIDATOR_EXT",
            "Severity": "LOGGER_SEVERITY_DEFAULT"
          }
        ],
        // "CloudWatchLogGroup": logGroupName,
        // "CloudWatchLogStream": logGroupStream
      },
      "ControlTablesSettings": {
        "historyTimeslotInMinutes": 5,
        "ControlSchema": "",
        "HistoryTimeslotInMinutes": 5,
        "HistoryTableEnabled": false,
        "SuspendedTablesTableEnabled": false,
        "StatusTableEnabled": false
      },
      "StreamBufferSettings": {
        "StreamBufferCount": 3,
        "StreamBufferSizeInMB": 8,
        "CtrlStreamBufferSizeInMB": 5
      },
      "ChangeProcessingDdlHandlingPolicy": {
        "HandleSourceTableDropped": true,
        "HandleSourceTableTruncated": true,
        "HandleSourceTableAltered": true
      },
      "ErrorBehavior": {
        "DataErrorPolicy": "LOG_ERROR",
        "DataTruncationErrorPolicy": "LOG_ERROR",
        "DataErrorEscalationPolicy": "SUSPEND_TABLE",
        "DataErrorEscalationCount": 0,
        "TableErrorPolicy": "SUSPEND_TABLE",
        "TableErrorEscalationPolicy": "STOP_TASK",
        "TableErrorEscalationCount": 0,
        "RecoverableErrorCount": -1,
        "RecoverableErrorInterval": 5,
        "RecoverableErrorThrottling": true,
        "RecoverableErrorThrottlingMax": 1800,
        "ApplyErrorDeletePolicy": "IGNORE_RECORD",
        "ApplyErrorInsertPolicy": "LOG_ERROR",
        "ApplyErrorUpdatePolicy": "LOG_ERROR",
        "ApplyErrorEscalationPolicy": "LOG_ERROR",
        "ApplyErrorEscalationCount": 0,
        "ApplyErrorFailOnTruncationDdl": false,
        "FullLoadIgnoreConflicts": true,
        "FailOnTransactionConsistencyBreached": false,
        "FailOnNoTablesCaptured": false
      },
      "ChangeProcessingTuning": {
        "BatchApplyPreserveTransaction": true,
        "BatchApplyTimeoutMin": 1,
        "BatchApplyTimeoutMax": 30,
        "BatchApplyMemoryLimit": 500,
        "BatchSplitSize": 0,
        "MinTransactionSize": 1000,
        "CommitTimeout": 1,
        "MemoryLimitTotal": 1024,
        "MemoryKeepTime": 60,
        "StatementCacheSize": 50
      },
      // "PostProcessingRules": null,
      // "CharacterSetSettings": null,
      // "LoopbackPreventionSettings": null,
      // "BeforeImageSettings": null
    };
  }
}

// for development, use account/region from cdk cli
const devEnv = {
  account: process.env.CDK_DEFAULT_ACCOUNT,
  region: process.env.CDK_DEFAULT_REGION,
};

const app = new App();

export interface DMSKinesisMonitorStackProps extends StackProps {
  readonly dmsReplicator: KinesisDMSReplicator;
}
export class DMSKinesisMonitorStack extends Stack {
  constructor(scope: Construct, id: string, props: DMSKinesisMonitorStackProps) {
    super(scope, id, props);
    
    new DmsDashboard(this, 'DMSDashboard', {
      collectionInterval: 1,
      numTasks: 3,
      replicationInstanceArn: props.dmsReplicator.replicatorInstance!.ref,
    });
  }
}

export interface DMSLobAlertingStackProps extends StackProps {
  readonly dmsReplicator: KinesisDMSReplicator;
}
export class DMSLobAlertingStack extends Stack {
  constructor(scope: Construct, id: string, props: DMSLobAlertingStackProps) {
    super(scope, id, props);
    
    new LobTruncationAlert(this, 'LobAlerting', {
      riArn: props.dmsReplicator.replicatorInstance!.ref,
      collectionInterval: 300,
      emailAddress: 'rridgley@amazon.com'
    });
  }
}

const dmsKinesisStack = new DMSKinesisStack(app, 'cdk-dms-kinesis-firehose-dev', { env: devEnv });

new DMSKinesisMonitorStack(app, 'cdk-dms-kinesis-firehose-dashboard-dev', {
  env: devEnv,
  dmsReplicator: dmsKinesisStack.dmsReplicator,
});

new DMSLobAlertingStack(app, 'cdk-dms-kinesis-firehose-lob-alerting-dev', {
  env: devEnv,
  dmsReplicator: dmsKinesisStack.dmsReplicator,  
})

const archDia = new ArchitectureDiagramAspect();
Aspects.of(dmsKinesisStack).add(archDia);
archDia.generateDiagram();

app.synth();