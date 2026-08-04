#!/usr/bin/env bash
# Local repro harness for the SOAL lineage Lambda (loglineage.py) — runs the real
# spark job through the locally-built image via spark-submit, bypassing the
# ECR-build -> deploy -> S3-upload -> CloudWatch loop.
#
# ponytail: hardcodes FRAMEWORK=ICEBERG, the non-prod profile/function, and the
# loglineage.py path — this Lambda only ever runs that one script/framework.
set -euo pipefail
cd "$(dirname "$0")"

PROFILE=non-prod
REGION=eu-west-1
FUNCTION_NAME=SparkOnAWSLambda-spark-on-lambda-stack
SPARK_SCRIPT="../../lambdas/python/soal_lineage/loglineage.py"
IMAGE=soal-local
ENV_FILE=soal.env

BUILD=false
REFRESH_ENV=false
for arg in "$@"; do
  case "$arg" in
    --build|-b) BUILD=true ;;
    --refresh-env) REFRESH_ENV=true ;;
    *) echo "Unknown flag: $arg" >&2; exit 1 ;;
  esac
done

if [ ! -f "$SPARK_SCRIPT" ]; then
  echo "Can't find spark script at $SPARK_SCRIPT (expected sibling ../../lambdas/python/soal_lineage/loglineage.py)" >&2
  exit 1
fi

if $BUILD || ! docker image inspect "$IMAGE" > /dev/null 2>&1; then
  echo "Building $IMAGE from repo root Dockerfile..."
  docker build --build-arg FRAMEWORK=ICEBERG --build-arg AWS_REGION="$REGION" -t "$IMAGE" ..
fi

if $REFRESH_ENV || [ ! -f "$ENV_FILE" ]; then
  echo "Pulling real runtime env from deployed $FUNCTION_NAME ($PROFILE/$REGION)..."
  aws lambda get-function-configuration \
    --function-name "$FUNCTION_NAME" \
    --profile "$PROFILE" --region "$REGION" \
    --query "Environment.Variables" | \
    jq -r 'to_entries[] | "\(.key)=\(.value)"' \
    > "$ENV_FILE"
  echo "AWS_REGION=$REGION" >> "$ENV_FILE"
  echo "AWS_DEFAULT_REGION=$REGION" >> "$ENV_FILE"
  echo "Wrote $ENV_FILE (re-run with --refresh-env to update)"
fi

echo "Exporting $PROFILE credentials..."
eval "$(aws configure export-credentials --profile "$PROFILE" --format env)"

echo "Running loglineage.py through spark-submit in $IMAGE..."
MSYS_NO_PATHCONV=1 docker run --rm \
  -v "$(pwd)/$SPARK_SCRIPT:/tmp/spark_script.py:ro" \
  -v "$(pwd)/event.json:/tmp/event.json:ro" \
  --env-file "$ENV_FILE" \
  -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN \
  --entrypoint spark-submit \
  "$IMAGE" /tmp/spark_script.py --event-file /tmp/event.json
