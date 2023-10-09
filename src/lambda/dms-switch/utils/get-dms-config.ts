import { CloudFormationClient } from '@aws-sdk/client-cloudformation';
import { listStackResources } from '../../dms-switch/utils/list-stack-resources';

export const getDmsConfig = async ({ cf, StackName }: {
  cf: CloudFormationClient;
  StackName: string;
}): Promise<string> => {
  const resources = await listStackResources({ cf, StackName, resources: [] });
  const dmsConfig = resources.filter((res) => res.ResourceType === "AWS::DMS::ReplicationConfig")[0].PhysicalResourceId;
  console.log(dmsConfig);
  return `${dmsConfig}`;
};