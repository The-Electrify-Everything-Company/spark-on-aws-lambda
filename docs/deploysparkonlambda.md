# SOAL (Spark on AWS Lambda) Stack Implementation

This document describes how the SOAL (Spark on AWS Lambda) CloudFormation stack (`cloudformation/sam-template.yaml`) is
implemented and deployed. A single template is shared by every environment *and* by both workloads
(lineage and WhatsApp, see below); per-environment/per-workload values (region, S3 bucket, image
repository, `WorkloadType`, and the parameter overrides below) live entirely in `samconfig.yaml` at the
repository root.

## Overview

`sam-template.yaml` is shared by every SOAL stack: non-prod and prod, and both the lineage and WhatsApp
workloads. It packages the Spark-on-Lambda container image (built by `sam-imagebuilder.yaml`, see
[Instructions in the wiki](https://github.com/aws-samples/spark-on-aws-lambda/wiki/Cloudformation)) into a
single AWS Lambda function, along with the supporting IAM role/policy and (optionally) VPC networking.

A `WorkloadType` parameter (`lineage` or `whatsapp`, default `lineage`) selects which IAM permissions the
Lambda role gets — see "IAM policy by workload" below — *and* which of the workload-specific environment
variables below are actually assigned to the Lambda. `SparkScript` still decides which script runs; the
`Environment.Variables` map wraps each workload-specific entry in an `Fn::If` on `IsLineage`/`IsWhatsapp`
that resolves to `AWS::NoValue` for the other workload, so CloudFormation omits the key entirely (it's
not just left as `''`) when it doesn't apply.

### Parameters

In addition to the existing image/script parameters, the template takes the following parameters that feed
the Lambda function's environment variables.

#### Common parameters

These are assigned unconditionally, regardless of `WorkloadType`:

| Parameter            | Mandatory (CFN)? | Environment variable   | Default    |
| --------------------- | ---------------- | ----------------------- | ---------- |
| `LambdaVersion`       | No               | `LAMBDA_VERSION`        | `''`       |
| `BuildTrigger`        | No               | `Rebuild`               | `v1`       |

There's also a `WorkloadType` parameter (default `lineage`, `AllowedValues: [lineage, whatsapp]`) that
selects the Lambda role's IAM permissions and the workload-specific env vars below (see "IAM policy by
workload" below), a `RoleName` parameter (no default — always mandatory, both workloads) that names the
IAM role, and a `BackupSuffix` parameter (default `''`) that combines with `RoleName` to form the role's
actual name (`${RoleName}${BackupSuffix}`) — `BackupSuffix` is a lighter-weight way to avoid role-name
collisions when standing up a second copy of the stack (e.g. during a migration) alongside an existing one.

`BackupSuffix` isn't limited to the role name — it's also folded into the deployed image reference and a
few env vars, so a non-empty `BackupSuffix` gives the second copy of the stack its own image tag and
storage locations, not just its own role:

- **`ImageUri`**: any existing `:tag` on the supplied `ImageUri` is stripped and replaced, so the Lambda
  actually deploys `<repo>${BackupSuffix}:latest` regardless of the tag passed in.
- **Env vars**: `SCRIPT_BUCKET` (from `ScriptBucket`), `WAREHOUSE_BUCKET` (from `WarehouseBucket`,
  lineage-only), `S3_BUCKET` (from `S3Bucket`, whatsapp-only), and `ICEBERG_TABLE_LOCATION` (from
  `IcebergTableLocation`, whatsapp-only) each get `${BackupSuffix}` appended.

No other env var (table names, workgroup, queue URL, etc.) or the Lambda function's own name picks up
`BackupSuffix`.

#### Lineage-only parameters

The following parameters are only assigned to the Lambda's environment when `WorkloadType=lineage`; on a
`whatsapp` deployment CloudFormation omits these keys from `Environment.Variables` entirely (via
`Fn::If`/`AWS::NoValue`), regardless of what value is passed in:

| Parameter            | Mandatory (CFN)? | Environment variable   | Default    |
| --------------------- | ---------------- | ----------------------- | ---------- |
| `WarehouseBucket`     | No               | `WAREHOUSE_BUCKET`      | `''`       |
| `CrTableName`         | No               | `CR_TABLE_NAME`         | `''`       |
| `DatabaseName`        | No               | `DATABASE_NAME`         | `''`       |
| `IcbWorkgroup`        | No               | `ICB_WG`                | `''`       |
| `RcTableName`         | No               | `RC_TABLE_NAME`         | `''`       |
| `LineageVerifyTable`  | No               | `LINEAGE_VERIFY_TABLE`  | `''`       |

CloudFormation still treats all of them as optional (each defaults to an empty string), so every
lineage environment's `samconfig.yaml` must set the ones it needs explicitly.

#### WhatsApp-only parameters

The following parameters are only assigned to the Lambda's environment when `WorkloadType=whatsapp`; on a
`lineage` deployment CloudFormation omits these keys from `Environment.Variables` entirely (via
`Fn::If`/`AWS::NoValue`), regardless of what value is passed in:

| Parameter             | Mandatory (CFN)? | Environment variable    | Default |
| ---------------------- | ---------------- | ------------------------ | ------- |
| `S3Bucket`            | No                | `S3_BUCKET`              | `''`    |
| `IcebergTableLocation`| No                | `ICEBERG_TABLE_LOCATION` | `''`    |
| `SqsQueueUrl`         | No                | `SQS_QUEUE_URL`          | `''`    |
| `GlueDatabase`        | No                | `GLUE_DATABASE`          | `''`    |
| `IcebergTable`        | No                | `ICEBERG_TABLE`          | `''`    |
| `S3ImagePrefix`       | No                | `S3_IMAGE_PREFIX`        | `''`    |

The WhatsApp access token is **not** a CloudFormation parameter or Lambda env var. CloudFormation's
`{{resolve:ssm-secure:...}}` dynamic reference — which would otherwise let CFN pull and decrypt an
SSM `SecureString` automatically — isn't supported for Lambda `Environment.Variables`, so instead
`soal_whatsapp_api_iceberg_write.py` fetches and decrypts the token itself at runtime (lazily, on
first use, then cached for the life of the execution environment) from a fixed AWS Systems Manager
Parameter Store `SecureString` named `/spark-on-lambda/whatsapp/access-token`, via the
`ssm:GetParameter`/`kms:Decrypt` permissions granted in `whatsapp-policy.yaml`. Before the first
`whatsapp-*` deploy in an account, seed the real value once:

```
aws ssm put-parameter --name /spark-on-lambda/whatsapp/access-token --type SecureString \
  --value '<whatsapp-api-access-token>' --region eu-west-1
```

Run this once per account (non-prod and prod each have their own Parameter Store) and again
whenever the token needs rotating — rotation takes effect on the next Lambda cold start, no stack
redeploy required.

### IAM policy by workload

`LambdaRole`'s attached policies depend on `WorkloadType`, in addition to policies attached regardless of
workload: `AWSLambdaBasicExecutionRole` (always), `AWSLambdaVPCAccessExecutionRole` (when
`AttachToVpc=True`), and any ARN passed via `SparkLambdapermissionPolicyArn`. Each workload's policy lives in its own
nested-stack template under `cloudformation/policies/`, referenced from `sam-template.yaml` via an
`AWS::CloudFormation::Stack` resource gated by a Condition (`IsLineage`/`IsWhatsapp`). Adding a new
workload means adding one `cloudformation/policies/<workload>-policy.yaml` file and one nested-stack
resource, not another inline policy block in `sam-template.yaml`'s `Resources:` section.

- **`lineage`** (`Condition: IsLineage`): `LineagePolicyStack` nested stack
  (`cloudformation/policies/lineage-policy.yaml`). `LambdaRole` gets the `AmazonDynamoDBFullAccess`
  managed policy plus this nested stack's policy (ECR pull, S3, scoped DynamoDB item actions, Lambda
  invoke, SQS, Athena, Glue, LakeFormation).
- **`whatsapp`** (`Condition: IsWhatsapp`): `WhatsappPolicyStack` nested stack
  (`cloudformation/policies/whatsapp-policy.yaml`). `LambdaRole` does *not* get
  `AmazonDynamoDBFullAccess`; instead it gets this nested stack's policy, which covers the same
  ECR/S3/Lambda-invoke/SQS/Glue/LakeFormation actions but with a wider set of scoped DynamoDB actions
  (including `CreateTable`/`DescribeTable`/`Query`/`Scan`) instead of the managed policy, and no
  Athena access (Athena isn't needed for this workload).

Both policies use `Resource: '*'` throughout (aside from the ECR statement, which is scoped to the
repository parsed out of `ImageUri`) — `WorkloadType` changes *which* policy is attached, not how tightly
scoped either one is. Each nested stack takes `ParentStackName`, `RoleName`, and `ImageUri` as parameters
(passed from `sam-template.yaml`) to name the policy and scope the ECR statement.

`samconfig.yaml`'s `resolve_s3: true` (set for every config-env) means SAM CLI auto-uploads these local
nested-stack template files during `sam deploy`/`sam package`, the same way it handles the container
image — no extra deploy commands are needed for them.

## Per-environment configuration: `samconfig.yaml`

Per-environment/per-workload values (region, stack name, image repository, and the parameter overrides
above) live in `samconfig.yaml` at the repository root (not inside `cloudformation/`), under a
`lineage-non-prod: deploy: parameters`, `lineage-prod: deploy: parameters`, `whatsapp-non-prod: deploy:
parameters`, or `whatsapp-prod: deploy: parameters` section — all four point at the same
`template_file: cloudformation/sam-template.yaml`, a path relative to the repository root (see
"Deploying" below for why commands are run from there).

**SAM CLI does not merge a `default: global: parameters` section into a named `--config-env` section** —
each environment section is loaded standalone (verify with `sam deploy --config-env lineage-non-prod
--debug` and check the "Configuration values are" log line). So `template_file`, `capabilities`, `resolve_s3`,
`region`, and `stack_name` must be repeated in full in every environment's own section, not just in
`default`. Use a YAML anchor (`&default_params` / `<<: *default_params`) to avoid re-typing the shared
keys, as shown below — this merges them at YAML-parse time, before SAM CLI ever sees the config, so it
still satisfies the "every section must be fully specified" rule. `default: global: parameters` only
applies on its own when running `sam deploy` with no `--config-env` at all.

`samconfig.yaml` is committed to the repository, at the root. To change a value for an environment,
edit its section directly — there's no need to touch the template itself.

```yaml
version: 0.1

default:
  global:
    parameters: &default_params
      template_file: cloudformation/sam-template.yaml
      capabilities: CAPABILITY_IAM CAPABILITY_NAMED_IAM
      resolve_s3: true

lineage-non-prod:
  deploy:
    parameters:
      <<: *default_params
      stack_name: spark-on-lambda-stack
      region: eu-west-1
      image_repository: <non-prod-ecr-repo>
      parameter_overrides:
        # Mandatory (no default in sam-template.yaml) - must be set
        - ScriptBucket=spark-on-lambda-non-prod
        - SparkScript=scripts/loglineage.py
        - ImageUri=<non-prod-ecr-repo>:latest
        - RoleName=soal-lineage-loger-role

        # Optional (has a default in sam-template.yaml) - only listed here to override
        - LambdaVersion=staging
        - WarehouseBucket=s3://apache-iceberg-datalineage-<non-prod-account>/
        # - WorkloadType=lineage
        # - BuildTrigger=v1
        # - BackupSuffix=
        # - LambdaFunctionPrefix=SparkOnAWSLambda
        # - LambdaTimeout=300
        # - LambdaMemory=1600
        # - SparkLambdapermissionPolicyArn=
        # - AttachToVpc=False
        # - SecurityGroupIds=
        # - SubnetIds=
        # - Command=sparkLambdaHandler.lambda_handler
        # - EntryPoint=
        # - WorkingDirectory=
        - CrTableName=iceberg_curated
        - DatabaseName=powerup-lakeformation
        - IcbWorkgroup=iceberg-workgroup
        - RcTableName=iceberg_records
        - LineageVerifyTable=lineage_verify

whatsapp-non-prod:
  deploy:
    parameters:
      <<: *default_params
      stack_name: spark-on-lambda-whatsapp-stack
      region: eu-west-1
      image_repository: <non-prod-ecr-repo>
      parameter_overrides:
        # Mandatory (no default in sam-template.yaml) - must be set
        - ScriptBucket=spark-on-lambda-non-prod
        - SparkScript=scripts/soal_whatsapp_api_iceberg_write.py
        - RoleName=soal_whatsapp_api_iceberg_write-role
        - ImageUri=<non-prod-ecr-repo>:latest
        - WorkloadType=whatsapp

        # Optional (has a default in sam-template.yaml) - only listed here to override
        # - LambdaFunctionPrefix=SparkOnAWSLambda
        # - LambdaTimeout=300
        # - LambdaMemory=1600
        # - SparkLambdapermissionPolicyArn=
        # - AttachToVpc=False
        # - SecurityGroupIds=
        # - SubnetIds=
        # - Command=sparkLambdaHandler.lambda_handler
        # - EntryPoint=
        # - WorkingDirectory=
        # - BackupSuffix=

        # WhatsApp-only parameters (optional to CloudFormation, but must be set for this workload)
        - S3Bucket=whatsapp-api-media
        - IcebergTableLocation=s3://apache-iceberg-whatsapp-api-webook
        - SqsQueueUrl=https://sqs.eu-west-1.amazonaws.com/<non-prod-account>/webhook_whatsapp_api_write
        - GlueDatabase=powerup-lakeformation
        - IcebergTable=webhook_whatsapp_api_messages
        - S3ImagePrefix=images/
```

The `lineage-prod` and `whatsapp-prod` sections follow the same shape as their non-prod counterparts, with
prod account/region/`stack_name`/`image_repository` values.

Note: `stack-name` and `region` are SAM CLI deploy options, not CloudFormation template parameters — they
must be set as top-level `stack_name`/`region` keys (as above), never inside `parameter_overrides`.
Putting `"stack-name=..."` or `"region=..."` in `parameter_overrides` is silently wrong (CloudFormation
parameter names can't contain hyphens) and produces `Error: Missing option '--stack-name'` since SAM CLI
never finds a real `stack_name` value. Likewise, don't add `"BackupSuffix="` (empty value) to
`parameter_overrides` — SAM CLI's `Key=Value` shorthand rejects an empty value; since `BackupSuffix`
already defaults to `''` in the template, just omit it unless you need a non-empty suffix. `RoleName` has
no default and must always be set explicitly in every config-env, for both workloads.

## Deploying

From the repository root (where `samconfig.yaml` lives), deploy any of the four stacks by config-env name:

```
sam deploy --config-env lineage-non-prod
sam deploy --config-env lineage-prod
sam deploy --config-env whatsapp-non-prod
sam deploy --config-env whatsapp-prod
```

`sam deploy` reads the matching `deploy: parameters` section for that environment from `samconfig.yaml`
and applies it — no need to pass `--parameter-overrides`, `--region`, `--s3-bucket`, or
`--image-repository` on the command line. The `whatsapp-*` config-envs deploy the same
`sam-template.yaml`, just with `WorkloadType=whatsapp` and the WhatsApp-only parameters set (see "IAM
policy by workload" and "WhatsApp-only parameters" above).

### Dry run (preview changes before deploying)

`sam deploy` has no dedicated `--dry-run` flag, but `--no-execute-changeset` gives the same effect: SAM
CLI creates the CloudFormation change set and prints/uploads it without executing it, so nothing in the
stack actually changes.

```
sam deploy --config-env lineage-non-prod --no-execute-changeset
sam deploy --config-env lineage-prod --no-execute-changeset
```

This still requires valid credentials and package/upload access (it builds the image, uploads artifacts,
and creates the change set in CloudFormation), so it's a true "what would this deploy do" preview rather
than a fully offline check. Review the change set (via the URL SAM CLI prints, or `aws cloudformation
describe-change-set --change-set-name <name> --stack-name <stack>` / the CloudFormation console) and then
either re-run `sam deploy --config-env <env>` without the flag to execute it, or delete the change set if
you don't want to proceed.

For a fully offline sanity check with no AWS calls at all (e.g. before even attempting a dry run), validate
the template syntax first:

```
sam validate --template-file cloudformation/sam-template.yaml --lint
```

## Repository layout

`cloudformation/` contains `sam-template.yaml`, `sam-imagebuilder.yaml`, and a `policies/` directory of
per-workload nested-stack templates (`lineage-policy.yaml`, `whatsapp-policy.yaml`). A single
`sam-template.yaml`, selected by `WorkloadType`, deploys all four stacks (lineage/WhatsApp ×
non-prod/prod) — there are no per-environment or per-workload copies of the template. `samconfig.yaml`
itself lives at the repository root, not inside `cloudformation/` (see "Per-environment configuration"
above).

## Building and publishing a new image / template version

Building the Docker image (`sam-imagebuilder.yaml`) and publishing the SAM application to the AWS
Serverless Application Repository are documented on the
[project wiki](https://github.com/aws-samples/spark-on-aws-lambda/wiki/Cloudformation).
