const { awscdk } = require('projen');
const project = new awscdk.AwsCdkTypeScriptApp({
  cdkVersion: '2.1.0',
  defaultReleaseBranch: 'main',
  name: 'cdk-dms-kinesis-firehose',
  devDeps: [
    '@aws-sdk/client-cloudformation',
    '@types/aws-lambda',
    '@aws-sdk/client-database-migration-service',
    '@aws-community/arch-dia',
    '@aws-cdk/aws-lambda-python-alpha',
  ]
  // deps: [],                /* Runtime dependencies of this module. */
  // description: undefined,  /* The description is just a string that helps people understand the purpose of the package. */
  // devDeps: [],             /* Build dependencies for this module. */
  // packageName: undefined,  /* The "name" in package.json. */
});
project.synth();