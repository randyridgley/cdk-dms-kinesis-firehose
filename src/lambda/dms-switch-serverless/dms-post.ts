import { CloudFormationClient } from "@aws-sdk/client-cloudformation";
import {
  DatabaseMigrationServiceClient,
  StartReplicationCommand,
} from "@aws-sdk/client-database-migration-service";
import type {
  CloudFormationCustomResourceEvent,
  CloudFormationCustomResourceFailedResponse,
  CloudFormationCustomResourceSuccessResponse,
} from "aws-lambda";
import { getDmsStatus } from "./utils/get-dms-status";
import { waitForDmsStatus } from "./utils/wait-for-dms-status";
import { hasDmsChanges } from "./utils/has-dms-changes";

const dms = new DatabaseMigrationServiceClient({});
const cf = new CloudFormationClient({});

export const handler = async (
  event: CloudFormationCustomResourceEvent
): Promise<
  | CloudFormationCustomResourceSuccessResponse
  | CloudFormationCustomResourceFailedResponse
> => {
  // note Delete shouldn't re-start DMS since the stack is being deleted
  const ReplicationConfigArn = `${process.env.DMS_TASK}`;
  console.log(JSON.stringify({ RequestType: event.RequestType }));
  try {
    switch (event.RequestType) {
      case "Create":
        const startCmd = new StartReplicationCommand({
          ReplicationConfigArn: ReplicationConfigArn,
          StartReplicationType: "start-replication",
        });
        await dms.send(startCmd);
        await waitForDmsStatus({
          dms,
          ReplicationConfigArn: ReplicationConfigArn,
          targetStatus: "running",
        });
        return { ...event, PhysicalResourceId: "post-dms", Status: "SUCCESS" };
      case "Update":
        let shouldUnpause = false;
        const dmsChanges = await hasDmsChanges({
          cf,
          StackName: `${process.env.STACK_NAME}`,
        });
        if (dmsChanges) {
          shouldUnpause = true;
        } else {
          const status = await getDmsStatus({ dms, ReplicationConfigArn: ReplicationConfigArn });
          console.log(`DMS status: ${status}`);
          if (status === "stopped" || status === "ready") {
            shouldUnpause = true;
          }
        }

        if (shouldUnpause) {
          // unpause DMS
          const startCmd = new StartReplicationCommand({
            ReplicationConfigArn: ReplicationConfigArn,
            StartReplicationType: "resume-processing",
          });
          await dms.send(startCmd);
          await waitForDmsStatus({
            dms,
            ReplicationConfigArn: ReplicationConfigArn,
            targetStatus: "running",
          });
        }
        return { ...event, PhysicalResourceId: "post-dms", Status: "SUCCESS" };

      default:
        console.error("No op for", event.RequestType);
        return { ...event, PhysicalResourceId: "post-dms", Status: "SUCCESS" };
    }
  } catch (e) {
    console.error(`Failed!`, e);
    return {
      ...event,
      PhysicalResourceId: "post-dms",
      Reason: (e as Error).message,
      Status: "FAILED",
    };
  }
};