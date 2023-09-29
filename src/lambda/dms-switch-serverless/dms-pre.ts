import { CloudFormationClient } from '@aws-sdk/client-cloudformation';
import { DatabaseMigrationServiceClient, StopReplicationCommand } from '@aws-sdk/client-database-migration-service';
import type {
  CloudFormationCustomResourceEvent,
  CloudFormationCustomResourceFailedResponse,
  CloudFormationCustomResourceSuccessResponse,
} from 'aws-lambda';
import { getDmsConfig } from './utils/get-dms-config';
import { hasDmsChanges } from './utils/has-dms-changes';
import { getDmsStatus } from './utils/get-dms-status';
import { waitForDmsStatus } from './utils/wait-for-dms-status';

const dms = new DatabaseMigrationServiceClient({});
const cf = new CloudFormationClient({});
let ReplicationConfigArn: string;

export const handler = async (
  event: CloudFormationCustomResourceEvent,
): Promise<CloudFormationCustomResourceSuccessResponse | CloudFormationCustomResourceFailedResponse> => {
  try {
    const StackName = `${process.env.STACK_NAME}`;
    if (!ReplicationConfigArn) {
      ReplicationConfigArn = await getDmsConfig({ cf, StackName });
    }
    const status = await getDmsStatus({ dms, ReplicationConfigArn: ReplicationConfigArn });
    if (status === 'running') {
      if (event.RequestType === 'Delete' || await hasDmsChanges({ cf, StackName })) {
        // pause task
        const stopCmd = new StopReplicationCommand({
          ReplicationConfigArn: ReplicationConfigArn,
        });
        await dms.send(stopCmd);
        // wait for task to be fully paused
        await waitForDmsStatus({ dms, ReplicationConfigArn: ReplicationConfigArn, targetStatus: 'stopped' });
      }
    }
    return { ...event, PhysicalResourceId: 'pre-dms', Status: 'SUCCESS' };
  } catch (e) {
    console.error(`Failed!`, e);
    return { ...event, PhysicalResourceId: 'pre-dms', Reason: (e as Error).message, Status: 'FAILED' };
  }
};