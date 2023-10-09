import { DatabaseMigrationServiceClient } from '@aws-sdk/client-database-migration-service';
import { getDmsTaskStatus } from './get-dms-status';

export const waitForDmsStatus = async ({
  dms,
  ReplicationTaskArn,
  targetStatus,
}: {
  dms: DatabaseMigrationServiceClient;
  ReplicationTaskArn: string;
  targetStatus: string;
}): Promise<string> => {
  let status = '';
  for (let j = 0; j < 24; j++) {
    status = await getDmsTaskStatus({ dms, ReplicationTaskArn });
    console.log(`DMS status: ${status}`);
    if (status === targetStatus) {
      return status;
    }
    await new Promise(f => setTimeout(f, 10000));
  }
  throw new Error(`DMS not ${targetStatus}: ${status}`);
};