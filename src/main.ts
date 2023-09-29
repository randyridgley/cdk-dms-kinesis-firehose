import { App, CfnOutput, Stack, StackProps } from 'aws-cdk-lib';
import { InterfaceVpcEndpointAwsService, Vpc } from 'aws-cdk-lib/aws-ec2';
import { EventType } from 'aws-cdk-lib/aws-s3';
import { SnsDestination } from 'aws-cdk-lib/aws-s3-notifications';
import { Topic } from 'aws-cdk-lib/aws-sns';
import { Construct } from 'constructs';
import { KinesisDMSReplicator } from './constructs/dms-replicator';
import { KinesisPipeline } from './constructs/kinesis-pipeline';
import { Notebook } from './constructs/notebook';
import { RDSPostgresDatabase } from './constructs/rds-postgres-db';

export class DMSKinesisStack extends Stack {
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

    new KinesisDMSReplicator(this, 'DMSReplicator', {
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
      serverless: true,
    });
  }
}

// for development, use account/region from cdk cli
const devEnv = {
  account: process.env.CDK_DEFAULT_ACCOUNT,
  region: process.env.CDK_DEFAULT_REGION,
};

const app = new App();

new DMSKinesisStack(app, 'cdk-dms-kinesis-firehose-dev', { env: devEnv });

app.synth();