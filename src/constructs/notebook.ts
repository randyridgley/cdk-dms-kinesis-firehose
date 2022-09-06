import { Fn } from "aws-cdk-lib";
import { Vpc } from "aws-cdk-lib/aws-ec2";
import { ManagedPolicy, Role, ServicePrincipal } from "aws-cdk-lib/aws-iam";
import { CfnNotebookInstance, CfnNotebookInstanceLifecycleConfig } from "aws-cdk-lib/aws-sagemaker";
import { Construct } from "constructs";
import { readFileSync } from "fs";
import { RDSPostgresDatabase } from "./rds-postgres-db";

export interface NotebookProps {
  vpc: Vpc,
  db: RDSPostgresDatabase
}

export class Notebook extends Construct {
  public readonly notebook: CfnNotebookInstance;

  constructor(scope: Construct, id: string, props: NotebookProps) {
    super(scope, id);
    let onStartScript = readFileSync('scripts/onStart.sh', 'utf8');
    let onCreateScript = readFileSync('scripts/onCreate.sh', 'utf8');

    /** Create the IAM Role to be used by SageMaker */
    const sagemakerRole = new Role(this, 'notebook-role', {
      assumedBy: new ServicePrincipal('sagemaker.amazonaws.com'),
      managedPolicies: [
        ManagedPolicy.fromAwsManagedPolicyName('AmazonSageMakerFullAccess'),
        ManagedPolicy.fromAwsManagedPolicyName('IAMReadOnlyAccess'),
      ],
    });

    /** Create the SageMaker Notebook Lifecycle Config */
    const lifecycleConfig = new CfnNotebookInstanceLifecycleConfig(this, 'LifecycleConfig', {
      notebookInstanceLifecycleConfigName: 'SagemakerLifecycleConfig',
      onCreate: [
        {
          content: Fn.base64(onCreateScript!),
        },
      ],
      onStart: [
        {
          content: Fn.base64(onStartScript!),
        },
      ],
    });

    this.notebook = new CfnNotebookInstance(this, 'SagemakerNotebook', {
      notebookInstanceName: 'replicationServiceNotebook',
      lifecycleConfigName: lifecycleConfig.notebookInstanceLifecycleConfigName,
      roleArn: sagemakerRole.roleArn,
      instanceType: 'ml.t2.medium',
      subnetId: props.vpc.privateSubnets[0].subnetId,
      securityGroupIds: [props.db.securitygroup.securityGroupId],
    });
  }
}