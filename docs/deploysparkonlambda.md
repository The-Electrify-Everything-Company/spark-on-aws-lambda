# SOAL (Spark on AWS Lambda) Stack Implementation

This document describes how the SOAL (Spark on AWS Lambda) CloudFormation stack (`cloudformation/sam-template.yaml`) is
implemented and deployed. A single template is shared by every environment *and* by both workloads
(lineage and WhatsApp, see below); per-environment/per-workload values (region, S3 bucket, image
repository, `WorkloadType`, and the parameter overrides below) live entirely in
`cloudformation/samconfig.yaml`, so there's no `Environment` CloudFormation parameter and no
per-environment tagging — nothing in the template itself needs to change to deploy to a different
environment or workload.

## Overview

`sam-template.yaml` is shared by every SOAL stack: non-prod and prod, and both the lineage and WhatsApp
workloads. It packages the Spark-on-Lambda container image (built by `sam-imagebuilder.yaml`, see
[Instructions in the wiki](https://github.com/aws-samples/spark-on-aws-lambda/wiki/Cloudformation)) into a
single AWS Lambda function, along with the supporting IAM role/policy and (optionally) VPC networking.

A `WorkloadType` parameter (`lineage` or `whatsapp`, default `lineage`) selects which IAM permissions the
Lambda role gets — see "IAM policy by workload" below. Which workload's environment variables actually
matter is decided by the script the Lambda runs (`SparkScript`), not by CloudFormation; `WorkloadType`
only controls IAM.

### Parameters

In addition to the existing image/script parameters, the template takes the following parameters that feed
the Lambda function's environment variables:

| Parameter            | Mandatory (CFN)? | Environment variable   | Default    |
| --------------------- | ---------------- | ----------------------- | ---------- |
| `LambdaVersion`       | No               | `LAMBDA_VERSION`        | `''`       |
| `WarehouseBucket`     | No               | `WAREHOUSE_BUCKET`      | `''`       |
| `CrTableName`         | No               | `CR_TABLE_NAME`         | `''`       |
| `DatabaseName`        | No               | `DATABASE_NAME`         | `''`       |
| `IcbWorkgroup`        | No               | `ICB_WG`                | `''`       |
| `RcTableName`         | No               | `RC_TABLE_NAME`         | `''`       |
| `LineageVerifyTable`  | No               | `LINEAGE_VERIFY_TABLE`  | `''`       |

These were all previously hardcoded in the template (e.g. `LAMBDA_VERSION: 'staging'`); they are now
`!Ref`'d from the template's `Parameters` block. CloudFormation treats all of them as optional (each
defaults to an empty string), but the template no longer bakes in any of the old hardcoded values — every
environment's `samconfig.yaml` must set all seven explicitly to get the previous behavior (e.g. a stack
deployed without `DatabaseName` set will get `DATABASE_NAME=''`, not `powerup-lakeformation`).

There's also a `WorkloadType` parameter (default `lineage`, `AllowedValues: [lineage, whatsapp]`) that
selects the Lambda role's IAM permissions (see "IAM policy by workload" below), a `RoleName` parameter
(no default — always mandatory, both workloads) that names the IAM role, and a `BackupSuffix` parameter
(default `''`) that combines with `RoleName` to form the role's actual name (`${RoleName}${BackupSuffix}`)
— `BackupSuffix` is a lighter-weight way to avoid role-name collisions when standing up a second copy of
the stack (e.g. during a migration) alongside an existing one.

#### WhatsApp-only parameters

The following parameters only matter when `WorkloadType=whatsapp` (they're still accepted, and default to
`''`, when deploying the lineage workload — they just go unused since the WhatsApp Lambda script is the
only thing that reads these env vars):

| Parameter             | Mandatory (CFN)? | Environment variable    | Default |
| ---------------------- | ---------------- | ------------------------ | ------- |
| `S3Bucket`            | No                | `S3_BUCKET`              | `''`    |
| `IcebergTableLocation`| No                | `ICEBERG_TABLE_LOCATION` | `''`    |
| `SqsQueueUrl`         | No                | `SQS_QUEUE_URL`          | `''`    |
| `GlueDatabase`        | No                | `GLUE_DATABASE`          | `''`    |
| `IcebergTable`        | No                | `ICEBERG_TABLE`          | `''`    |
| `S3ImagePrefix`       | No                | `S3_IMAGE_PREFIX`        | `''`    |
| `WhatsappAccessToken` | No                | `WHATSAPP_ACCESS_TOKEN`  | `''` (`NoEcho`) |

`WhatsappAccessToken` is declared `NoEcho: True`, so CloudFormation masks it in the console/CLI/describe
calls (it's still visible in plaintext in `samconfig.yaml`, so treat that file as containing a secret).

### IAM policy by workload

`LambdaRole`'s attached policies depend on `WorkloadType`:

- **`lineage`** (`Condition: IsLineage`): gets the `AmazonDynamoDBFullAccess` managed policy plus the
  `LambdaPolicy` inline policy (ECR pull, S3, scoped DynamoDB item actions, Lambda invoke, SQS, Athena,
  Glue, LakeFormation).
- **`whatsapp`** (`Condition: IsWhatsapp`): does *not* get `AmazonDynamoDBFullAccess`; instead gets the
  `LambdaPolicyWhatsapp` inline policy, which covers the same ECR/S3/Lambda-invoke/SQS/Glue/LakeFormation
  actions but with a wider set of scoped DynamoDB actions (including `CreateTable`/`DescribeTable`/
  `Query`/`Scan`) instead of the managed policy, and no Athena access (Athena isn't needed for this
  workload).

Both inline policies use `Resource: '*'` throughout (aside from the ECR statement, which is scoped to the
repository parsed out of `ImageUri`) — `WorkloadType` changes *which* policy is attached, not how tightly
scoped either one is.

## Per-environment configuration: `samconfig.yaml`

Per-environment/per-workload values (region, stack name, image repository, and the parameter overrides
above) live in `cloudformation/samconfig.yaml`, under a `lineage-non-prod: deploy: parameters`,
`lineage-prod: deploy: parameters`, `whatsapp-non-prod: deploy: parameters`, or `whatsapp-prod: deploy:
parameters` section — all four point at the same `template_file: sam-template.yaml`.

**SAM CLI does not merge a `default: global: parameters` section into a named `--config-env` section** —
each environment section is loaded standalone (verify with `sam deploy --config-env lineage-non-prod
--debug` and check the "Configuration values are" log line). So `template_file`, `capabilities`, `resolve_s3`,
`region`, and `stack_name` must be repeated in full in every environment's own section, not just in
`default`. Use a YAML anchor (`&default_params` / `<<: *default_params`) to avoid re-typing the shared
keys, as shown below — this merges them at YAML-parse time, before SAM CLI ever sees the config, so it
still satisfies the "every section must be fully specified" rule. `default: global: parameters` only
applies on its own when running `sam deploy` with no `--config-env` at all.

`samconfig.yaml` is not committed — it isn't tracked in git since it holds account-specific values like
ECR repository URIs, S3 bucket names, and (for the WhatsApp config-envs) the plaintext
`WhatsappAccessToken`. Each developer/deployment target maintains their own local copy; use the shape
below as a template for creating one.

> **Note:** `.gitignore` has a `samconfig.*` entry, so `samconfig.yaml` (as well as `samconfig.toml`) stays
> untracked automatically — no need to add it yourself or double check before a broad `git add`.

To change a value for an environment, edit its section in your local `samconfig.yaml` — there's no need to
touch the template itself.

```yaml
version: 0.1

default:
  global:
    parameters: &default_params
      template_file: sam-template.yaml
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
        - WhatsappAccessToken=<whatsapp-api-access-token>
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

From the `cloudformation` directory, deploy any of the four stacks by config-env name:

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
sam validate --template-file sam-template.yaml --lint
```

## History: removed legacy templates

Earlier iterations of the WhatsApp stack lived in their own templates: first per-environment
(`sam-template-whatsapp-api-non-prod.yaml`, `-prod.yaml`, and their `.example.yaml` counterparts), then
consolidated into a single `sam-template-whatsapp-api.yaml` shared across environments. There was also a
separate `sam-template.prod.yaml` for the lineage workload before it was folded into the single
`sam-template.yaml`. All of these were superseded by the `WorkloadType` parameter on `sam-template.yaml`
described above — `samconfig.yaml`'s `whatsapp-non-prod`/`whatsapp-prod` config-envs deploy
`sam-template.yaml` with `WorkloadType=whatsapp`, not a separate template — and have since been deleted
from the repository. `cloudformation/` now contains only `sam-template.yaml` and `sam-imagebuilder.yaml`.

## Building and publishing a new image / template version

Building the Docker image (`sam-imagebuilder.yaml`) and publishing the SAM application to the AWS
Serverless Application Repository are unchanged from before and are documented on the
[project wiki](https://github.com/aws-samples/spark-on-aws-lambda/wiki/Cloudformation).
