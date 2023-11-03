import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as iam from 'aws-cdk-lib/aws-iam';
import { DockerImageFunction, DockerImageCode } from 'aws-cdk-lib/aws-lambda';
import * as path from 'path';
import { Bucket, EventType } from 'aws-cdk-lib/aws-s3';
import { Table, AttributeType } from 'aws-cdk-lib/aws-dynamodb';
import { LambdaDestination } from 'aws-cdk-lib/aws-s3-notifications';

export class SimpleCsvEtlStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const bucket = this.createCsvBucket();
    const dynamoDbTable = this.createDynamoDbTable();
    const role = this.createLambdaRole(bucket, dynamoDbTable);
    const lambda = this.createLambda(role, dynamoDbTable.tableName);
    this.createLambdaTrigger(lambda, bucket);
  }

  private createCsvBucket() {
    const bucket = new Bucket(this, "CDKSimpleCsvS3Bucket", {
      versioned: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true
    });
    return bucket;
  }

  private createDynamoDbTable() {
    const table = new Table(this, "CDKSimpleCsvEtlDynamoDbTable", {
      partitionKey: {
        name: "Year",
        type: AttributeType.NUMBER
      },
      sortKey: {
        name: "Title",
        type: AttributeType.STRING
      },
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      tableName: "MoviesTable",
      writeCapacity: 100,
      billingMode: cdk.aws_dynamodb.BillingMode.PROVISIONED
    });
    return table;
  }

  private createLambdaRole(bucket: cdk.aws_s3.Bucket, dynamoDbTable: cdk.aws_dynamodb.Table) {
    const role = new iam.Role(this, "CDKLambdaRoleCsvEtl", {
      assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
      roleName: "simple-csv-etl-lambda-role",
      description: "Creating role for simple lambda csv etl",
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSLambdaBasicExecutionRole")
      ],
      inlinePolicies: {
        "SimpleCsvEtlPolicies": new iam.PolicyDocument({
          statements: [
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: [
                "s3:GetObject"
              ],
              resources: [
                bucket.arnForObjects("*")
              ]
            }),
            new iam.PolicyStatement({
              effect: iam.Effect.ALLOW,
              actions: [
                "dynamodb:BatchWriteItem"
              ],
              resources: [
                dynamoDbTable.tableArn
              ]
            })
          ]
        })
      }
    });

    role.applyRemovalPolicy(cdk.RemovalPolicy.DESTROY);
    return role;
  }

  private createLambda(role: cdk.aws_iam.Role, tableName: string) {
    const lambda = new DockerImageFunction(this, "CDKSimpleCSVEtl", {
      code: DockerImageCode.fromImageAsset(
        path.join(__dirname, "..", "lambda")
      ),
      functionName: "simple-csv-etl-lambda",
      timeout: cdk.Duration.minutes(10),
      role: role,
      environment: {
        "AWS_MOVIES_TABLE_NAME": tableName
      }
    });

    lambda.applyRemovalPolicy(cdk.RemovalPolicy.DESTROY);
    return lambda;
  }

  private createLambdaTrigger(lambda: DockerImageFunction, bucket: Bucket) {
    bucket.addEventNotification(
      EventType.OBJECT_CREATED_PUT,
      new LambdaDestination(lambda)
    )
  }
}
