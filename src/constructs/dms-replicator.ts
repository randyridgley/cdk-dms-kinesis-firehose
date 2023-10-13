import { CfnEndpoint, CfnReplicationConfig, CfnReplicationInstance, CfnReplicationSubnetGroup, CfnReplicationTask } from "aws-cdk-lib/aws-dms";
import { SecurityGroup, SubnetType, Vpc } from "aws-cdk-lib/aws-ec2";
import { Effect, PolicyStatement, Role, ServicePrincipal } from "aws-cdk-lib/aws-iam";
import { Stream } from "aws-cdk-lib/aws-kinesis";
import { Construct } from "constructs";
import { RDSPostgresDatabase } from "./rds-postgres-db";
import { SecureBucket } from "./secure-bucket";
import { Provider } from "aws-cdk-lib/custom-resources";
import { CustomResource, Duration, Stack } from "aws-cdk-lib";
import { join } from "path";
import { Runtime } from "aws-cdk-lib/aws-lambda";
import { RetentionDays } from "aws-cdk-lib/aws-logs";
import { PythonFunction, PythonLayerVersion } from "@aws-cdk/aws-lambda-python-alpha";

export interface DMSReplicatorProps {
  taskSettings: any;
  source: SourceProps,
  target: TargetProps,
  vpc: Vpc,
  serverless: boolean,
}

export interface TargetProps {
  stream: Stream,
  bucket: SecureBucket,
}

export interface SourceProps {
  databaseName: string,
  port: number,
  serverName: string,
  username: string,
  password: string,
  sourceDB: RDSPostgresDatabase,
}

export class KinesisDMSReplicator extends Construct {
  readonly task?: CfnReplicationTask;
  readonly replicationConfig?: CfnReplicationConfig;
  readonly replicatorInstance?: CfnReplicationInstance;

  constructor(scope: Construct, id: string, props: DMSReplicatorProps) {
    super(scope, id);

    const subnetGroup = new CfnReplicationSubnetGroup(this, 'DmsSubnetGroup', {
      subnetIds: props.vpc.selectSubnets({
        subnetType: SubnetType.PRIVATE_WITH_EGRESS
      }).subnetIds,
      replicationSubnetGroupDescription: 'Replication Subnet group'
    });

    const dmsSecurityGroup = new SecurityGroup(this, 'DmsSecurityGroup', {
      vpc: props.vpc
    });

    props.source.sourceDB.database.connections.allowDefaultPortFrom(dmsSecurityGroup);

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
      username: props.source.username,
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

    const pythonLayer = new PythonLayerVersion(this, 'python-layer', {
      entry: 'src/lambda/dms-switch-py/layer/utils',      
      compatibleRuntimes: [Runtime.PYTHON_3_11],
      bundling: {
        outputPathSuffix: '/python',
      }
    })
    
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

    const lambdaProps = {
      runtime: Runtime.PYTHON_3_11,
      memorySize: 1028,
      timeout: Duration.minutes(15),
      logRetention: RetentionDays.ONE_DAY,
      layers: [pythonLayer],
    };

    if (!props.serverless) {
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

      this.task = new CfnReplicationTask(this, 'KinesisReplicationTask', {
        replicationInstanceArn: this.replicatorInstance.ref,
        migrationType: 'full-load-and-cdc',
        sourceEndpointArn: source.ref,
        targetEndpointArn: target.ref,
        tableMappings: JSON.stringify(dmsTableMappings),
        replicationTaskSettings: JSON.stringify(props.taskSettings),
      });
      
      const preDmsFn = new PythonFunction(this, `pre-dms`, {
        ...lambdaProps,
        entry: join(__dirname, "../lambda/dms-switch-py/dms_pre/"),
        environment: {
          STACK_NAME: Stack.of(this).stackName,
        },        
        initialPolicy: [
          new PolicyStatement({
            effect: Effect.ALLOW,
            actions: [
              "cloudformation:Describe*",
              "cloudformation:Get*",
              "cloudformation:List*",
            ],
            resources: ["*"],
          }),
          new PolicyStatement({
            effect: Effect.ALLOW,
            actions: ["dms:*"],
            resources: ["*"],
          }),
        ],
      });
  
      const postDmsFn = new PythonFunction(this, `post-dms`, {
        ...lambdaProps,
        entry: join(__dirname, "../lambda/dms-switch-py/dms_post/"),
        environment: {
          STACK_NAME: Stack.of(this).stackName,
          DMS_TASK: this.task.ref,
        },
        initialPolicy: [
          new PolicyStatement({
            effect: Effect.ALLOW,
            actions: [
              "cloudformation:Describe*",
              "cloudformation:Get*",
              "cloudformation:List*",
            ],
            resources: ["*"],
          }),
          new PolicyStatement({
            effect: Effect.ALLOW,
            actions: ["dms:*"],
            resources: ["*"],
          }),
        ],
      });
            
      const preProvider = new Provider(this, `pre-dms-provider`, {
        onEventHandler: preDmsFn,
      });
  
      const preResource = new CustomResource(this, `pre-dms-resource`, {
        properties: { Version: new Date().getTime().toString() },
        serviceToken: preProvider.serviceToken,
      });
  
      const postProvider = new Provider(this, `post-dms-provider`, {
        onEventHandler: postDmsFn,
      });
  
      const postResource = new CustomResource(this, `post-dms-resource`, {
        properties: { Version: new Date().getTime().toString() },
        serviceToken: postProvider.serviceToken,
      });

      this.task.node.addDependency(preResource);
      postResource.node.addDependency(this.task);

    } else {
      this.replicationConfig = new CfnReplicationConfig(this, 'ServerlessReplicationConfig', {
        computeConfig: {
          maxCapacityUnits: 16,    
          multiAz: false,
          replicationSubnetGroupId: subnetGroup.ref,
          vpcSecurityGroupIds: [dmsSecurityGroup.securityGroupId],
        },
        replicationSettings: props.taskSettings,
        replicationType: 'full-load-and-cdc',
        sourceEndpointArn: source.ref,
        tableMappings: dmsTableMappings,
        targetEndpointArn: target.ref,
        replicationConfigIdentifier: 'dmsKinesisConfig',
      });

      const preDmsServerlessFn = new PythonFunction(this, `pre-dms`, {
        ...lambdaProps,
        entry: join(__dirname, "../lambda/dms-switch-py/dms_pre_serverless/"),
        environment: {
          STACK_NAME: Stack.of(this).stackName,
        },
        initialPolicy: [
          new PolicyStatement({
            effect: Effect.ALLOW,
            actions: [
              "cloudformation:Describe*",
              "cloudformation:Get*",
              "cloudformation:List*",
            ],
            resources: ["*"],
          }),
          new PolicyStatement({
            effect: Effect.ALLOW,
            actions: ["dms:*"],
            resources: ["*"],
          }),
        ],
      });
  
      const postDmsServerlessFn = new PythonFunction(this, `post-dms`, {
        ...lambdaProps,
        entry: join(__dirname, "../lambda/dms-switch/dms_post_serverless/"),
        environment: {
          STACK_NAME: Stack.of(this).stackName,
          DMS_TASK: this.replicationConfig.ref, // 
        },
        initialPolicy: [
          new PolicyStatement({
            effect: Effect.ALLOW,
            actions: [
              "cloudformation:Describe*",
              "cloudformation:Get*",
              "cloudformation:List*",
            ],
            resources: ["*"],
          }),
          new PolicyStatement({
            effect: Effect.ALLOW,
            actions: ["dms:*"],
            resources: ["*"],
          }),
        ],
      });
            
      const preProvider = new Provider(this, `pre-dms-provider`, {
        onEventHandler: preDmsServerlessFn,
      });
  
      const preResource = new CustomResource(this, `pre-dms-resource`, {
        properties: { Version: new Date().getTime().toString() },
        serviceToken: preProvider.serviceToken,
      });
  
      const postProvider = new Provider(this, `post-dms-provider`, {
        onEventHandler: postDmsServerlessFn,
      });
  
      const postResource = new CustomResource(this, `post-dms-resource`, {
        properties: { Version: new Date().getTime().toString() },
        serviceToken: postProvider.serviceToken,
      });

      this.replicationConfig.node.addDependency(preResource);
      postResource.node.addDependency(this.replicationConfig);
    }
  }
}  