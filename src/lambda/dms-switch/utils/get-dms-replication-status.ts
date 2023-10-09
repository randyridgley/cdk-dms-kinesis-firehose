import {
  DatabaseMigrationServiceClient,
  DescribeReplicationsCommand,
} from '@aws-sdk/client-database-migration-service';

export const getDmsReplicationStatus = async ({
  dms,
  ReplicationConfigArn: replicationConfigArn,
}: {
  dms: DatabaseMigrationServiceClient;
  ReplicationConfigArn: string;
}): Promise<string> => {
  const configDescriptionCmd = new DescribeReplicationsCommand({
    Filters: [
      {
        Name: 'replication-config-arn',
        Values: [replicationConfigArn],
      },
    ],
  });
  const taskDescription = await dms.send(configDescriptionCmd);
  
  const status = `${taskDescription?.Replications?.[0].Status}`;
  return status;
};