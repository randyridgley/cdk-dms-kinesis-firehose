// import { RemovalPolicy } from "aws-cdk-lib";
import { CfnEndpoint, CfnReplicationInstance, CfnReplicationSubnetGroup, CfnReplicationTask } from "aws-cdk-lib/aws-dms";
import { SecurityGroup, SubnetType, Vpc } from "aws-cdk-lib/aws-ec2";
import { Role, ServicePrincipal } from "aws-cdk-lib/aws-iam";
import { Stream } from "aws-cdk-lib/aws-kinesis";
// import { LogGroup, LogStream, RetentionDays } from "aws-cdk-lib/aws-logs";
// import { AwsCustomResource, AwsCustomResourcePolicy, PhysicalResourceId } from "aws-cdk-lib/custom-resources";
import { Construct } from "constructs";
import { RDSPostgresDatabase } from "./rds-postgres-db";
import { SecureBucket } from "./secure-bucket";

export interface DMSReplicatorProps {
  source: SourceProps;
  target: TargetProps;
  vpc: Vpc;
}

export interface TargetProps {
  stream: Stream;
  bucket: SecureBucket
}

export interface SourceProps {
  databaseName: string;
  port: number,
  serverName: string,
  username: string,
  password: string,
  sourceDB: RDSPostgresDatabase
}

export class DMSReplicator extends Construct {
  public readonly replicatorInstance: CfnReplicationInstance;

  constructor(scope: Construct, id: string, props: DMSReplicatorProps) {
    super(scope, id);

    // const dmsLogGroup = new LogGroup(this, 'DMSLogGroup', {
    //   retention: RetentionDays.ONE_WEEK,
    //   removalPolicy: RemovalPolicy.DESTROY,
    // });

    // const dmsLogStream = new LogStream(this, 'DMSLogStream', {
    //   logGroup: dmsLogGroup,
    //   removalPolicy: RemovalPolicy.DESTROY,
    // });

    const subnetGroup = new CfnReplicationSubnetGroup(this, 'DmsSubnetGroup', {
      subnetIds: props.vpc.selectSubnets({
        subnetType: SubnetType.PRIVATE_WITH_NAT
      }).subnetIds,
      replicationSubnetGroupDescription: 'Replication Subnet group'
    });

    const dmsSecurityGroup = new SecurityGroup(this, 'DmsSecurityGroup', {
      vpc: props.vpc
    });

    props.source.sourceDB.database.connections.allowDefaultPortFrom(dmsSecurityGroup);
    this.replicatorInstance = new CfnReplicationInstance(this, 'DmsInstance', {
      replicationInstanceClass: 'dms.r5.large',
      allocatedStorage: 10,
      allowMajorVersionUpgrade: false,
      autoMinorVersionUpgrade: false,
      multiAz: false,
      publiclyAccessible: false,
      replicationSubnetGroupIdentifier: subnetGroup.ref,
      vpcSecurityGroupIds: [dmsSecurityGroup.securityGroupId],      
    });

    const streamWriterRole = new Role(this, 'DMSStreamRole', {
      assumedBy: new ServicePrincipal('dms.amazonaws.com')
    });

    props.target.stream.grantReadWrite(streamWriterRole);

    const source = new CfnEndpoint(this, 'Source', {
      endpointType: 'source',
      engineName: 'postgres',
      databaseName: props.source.databaseName,
      password: props.source.password,
      port: props.source.port,
      serverName: props.source.serverName,
      username: props.source.username
    });

    const target = new CfnEndpoint(this, 'dms-target', {
      endpointType: 'target',
      engineName: 'kinesis',
      kinesisSettings: {
        messageFormat: 'JSON',
        streamArn: props.target.stream.streamArn,
        serviceAccessRoleArn: streamWriterRole.roleArn
      },      
    });

    const dmsTargetRole = new Role(this, 'dmsTargetRole', {
      assumedBy: new ServicePrincipal('dms.amazonaws.com')
    });
    props.target.bucket.grantReadWrite(dmsTargetRole);

    new CfnEndpoint(this, 'Target', {
      endpointType: 'target',
      engineName: 's3',
      extraConnectionAttributes: 'dataFormat=parquet;',
      s3Settings: {
        bucketName: props.target.bucket.bucketName,
        serviceAccessRoleArn: dmsTargetRole.roleArn
      }
    });

    var dmsTableMappings = {
      "rules": [
        {
          "rule-type": "selection",
          "rule-id": "1",
          "rule-name": "1",
          "object-locator": {
            "schema-name": "public",
            "table-name": "%"
          },
          "rule-action": "include"
        }
      ]
    };

    new CfnReplicationTask(this, 'KinesisReplicationTask', {
      replicationInstanceArn: this.replicatorInstance.ref,
      migrationType: 'full-load-and-cdc',
      sourceEndpointArn: source.ref,
      targetEndpointArn: target.ref,
      tableMappings: JSON.stringify(dmsTableMappings),
      // replicationTaskSettings: JSON.stringify(this.getTaskSettings())      
    });

    new CfnReplicationTask(this, 'S3ReplicationTask', {
      migrationType: 'full-load-and-cdc',
      replicationInstanceArn: this.replicatorInstance.ref,
      sourceEndpointArn: source.ref,
      tableMappings: JSON.stringify(dmsTableMappings),
      targetEndpointArn: target.ref,
      replicationTaskSettings: JSON.stringify(this.getTaskSettings())
    });

    // new AwsCustomResource(this, 'StartTask', {
    //   onCreate: {
    //     service: 'DMS',
    //     action: 'startReplicationTask',
    //     parameters: {
    //       ReplicationTaskArn: replicationTask.ref,
    //       StartReplicationTaskType: "start-replication"
    //     },
    //     physicalResourceId: PhysicalResourceId.fromResponse('ReplicationTask.ReplicationTaskIdentifier'),        
    //   },
    //   policy: AwsCustomResourcePolicy.fromSdkCalls({ resources: AwsCustomResourcePolicy.ANY_RESOURCE })
    // });
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
      "PostProcessingRules": null,
      "CharacterSetSettings": null,
      "LoopbackPreventionSettings": null,
      "BeforeImageSettings": null
    };
  }
}  