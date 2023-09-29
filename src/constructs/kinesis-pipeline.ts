import { CfnResource, Duration, RemovalPolicy } from "aws-cdk-lib";
import { Effect, PolicyDocument, PolicyStatement, Role, ServicePrincipal } from "aws-cdk-lib/aws-iam";
import { Stream, StreamMode } from "aws-cdk-lib/aws-kinesis";
import { CfnDeliveryStream } from "aws-cdk-lib/aws-kinesisfirehose";
import { LogGroup, LogStream, RetentionDays } from "aws-cdk-lib/aws-logs";
import { Construct } from "constructs";
import { SecureBucket } from "./secure-bucket";

export class KinesisPipeline extends Construct {
  public readonly bucket: SecureBucket;
  public readonly stream: Stream;
  public readonly firehoseLogGroup: LogGroup;
  public readonly deliveryStream: CfnDeliveryStream;

  constructor(scope: Construct, id: string) {
    super(scope, id);

    this.bucket = new SecureBucket(this, 'EventsBucket', {
      lifecycleRules: [
        {
          expiration: Duration.days(45),
        },
      ],
    });

    this.stream = new Stream(this, 'EventsStream', {
      retentionPeriod: Duration.days(30),
      streamMode: StreamMode.ON_DEMAND,
    });

    this.firehoseLogGroup = new LogGroup(this, 'FirehoseLogGroup', {
      retention: RetentionDays.ONE_WEEK,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    const firehoseLogStream = new LogStream(this, 'FirehoseLogStream', {
      logGroup: this.firehoseLogGroup,
      removalPolicy: RemovalPolicy.DESTROY,
    });

    // had to hack the inline kinesis:DescribeStream as creation of resources was out of order.
    const firehoseKinesisReaderRole = new Role(this, 'FirehoseReader', {
      assumedBy: new ServicePrincipal('firehose.amazonaws.com'),
      description: 'Role for Firehose to read from Kinesis Stream  on storage layer',
      inlinePolicies: {
        'allow-s3-kinesis-logs': new PolicyDocument({
          statements: [
            new PolicyStatement({
              effect: Effect.ALLOW,
              actions: [
                'kinesis:DescribeStream',
                'kinesis:DescribeStreamSummary',
                'kinesis:GetRecords',
                'kinesis:GetShardIterator',
                'kinesis:ListShards',
                'kinesis:SubscribeToShard',
              ],
              resources: [this.stream!.streamArn],
            }),
          ],
        }),
      },
    });

    const firehoseS3WriterRole = new Role(this, 'FirehoseWriter', {
      assumedBy: new ServicePrincipal('firehose.amazonaws.com'),
      description: 'Role for Firehose to write to S3 storage layer',
    });
    this.bucket.grantWrite(firehoseS3WriterRole);

    this.deliveryStream = new CfnDeliveryStream(this, 'EventsDeliveryStream', {
      deliveryStreamType: 'KinesisStreamAsSource',
      kinesisStreamSourceConfiguration: {
        kinesisStreamArn: this.stream.streamArn,
        roleArn: firehoseKinesisReaderRole.roleArn,
      },
      extendedS3DestinationConfiguration: {
        bucketArn: this.bucket.bucketArn,
        roleArn: firehoseS3WriterRole.roleArn,
        bufferingHints: {
          intervalInSeconds: 60,
          sizeInMBs: 128,
        },
        compressionFormat: 'UNCOMPRESSED',
        cloudWatchLoggingOptions: {
          enabled: true,
          logGroupName: this.firehoseLogGroup.logGroupName,
          logStreamName: firehoseLogStream.logStreamName,
        },
        prefix: 'processed/',
        errorOutputPrefix: 'error/',
      },
    });

    this.deliveryStream.node.addDependency(firehoseKinesisReaderRole.node.defaultChild as CfnResource);
    this.deliveryStream.node.addDependency(firehoseS3WriterRole.node.defaultChild as CfnResource);
    this.stream.grantRead(firehoseKinesisReaderRole);
    // Work around this: https://github.com/aws/aws-cdk/issues/10783
    const grant = this.stream.grant(firehoseKinesisReaderRole, 'kinesis:DescribeStream');
    grant.applyBefore(this.deliveryStream);
    this.firehoseLogGroup.grantWrite(firehoseS3WriterRole);
  }
}  