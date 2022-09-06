import { AnyPrincipal, Effect, PolicyStatement } from 'aws-cdk-lib/aws-iam';
import { BlockPublicAccess, Bucket, BucketAccessControl, BucketEncryption, BucketProps } from 'aws-cdk-lib/aws-s3';
import { Construct } from 'constructs';

const defaults = {
  blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
  encryption: BucketEncryption.S3_MANAGED,
  enforceSSL: true,
  accessControl: BucketAccessControl.LOG_DELIVERY_WRITE,
};

export class SecureBucket extends Bucket {
  constructor(scope: Construct, id: string, props: BucketProps) {
    super(scope, id, { ...defaults, ...props });
    this.addToResourcePolicy(
      new PolicyStatement({
        principals: [new AnyPrincipal()],
        effect: Effect.DENY,
        actions: ['s3:*'],
        conditions: {
          Bool: { 'aws:SecureTransport': false },
        },
        resources: [this.bucketArn, this.bucketArn + '/*'],
      }),
    );
  }
}
