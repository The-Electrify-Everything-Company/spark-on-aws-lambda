import importlib
import json
import boto3
import sys
import os
import subprocess
import logging
import tempfile

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

def s3_script_download(s3_bucket_script: str,input_script: str)-> None:
    """
    """
    s3_client = boto3.resource("s3")

    try:
        logger.info(f'Now downloading script {input_script} in {s3_bucket_script} to /tmp')
        s3_client.Bucket(s3_bucket_script).download_file(input_script, "/tmp/spark_script.py")
      
    except Exception as e :
        logger.error(f'Error downloading the script {input_script} in {s3_bucket_script}: {e}')
    else:
        logger.info(f'Script {input_script} successfully downloaded to /tmp')

def import_spark_script(path: str, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module

def spark_submit(s3_bucket_script: str,input_script: str, event: dict)-> None:
    """
    Submits a local Spark script using spark-submit.
    """
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".json", dir="/tmp") as tmpfile:
        json.dump(event, tmpfile)
        tmpfile_path = tmpfile.name
    try:
        logger.info(f'Spark-Submitting the Spark script {input_script} from {s3_bucket_script}')
        result = subprocess.run(
            ["spark-submit", "/tmp/spark_script.py", "--event-file", tmpfile_path],
            check=True,
            env=os.environ,
            capture_output=True,
            text=True
        )
        logger.info("=== SUBPROCESS STDOUT ===")
        logger.info(result.stdout)

        logger.info("=== SUBPROCESS STDERR ===")
        logger.error(result.stderr)
    except Exception as e :
        logger.error(f'Error Spark-Submit with exception: {e}')
        raise e
    finally:
        logger.info(f'Script {input_script} successfully submitted')
        os.remove(tmpfile_path)

def lambda_handler(event, context):

    """
    Lambda_handler is called when the AWS Lambda
    is triggered. The function is downloading file 
    from Amazon S3 location and spark submitting 
    the script in AWS Lambda
    """

    logger.info("******************Start AWS Lambda Handler************")
    
    s3_bucket_script = os.environ['SCRIPT_BUCKET']
    input_script = os.environ['SPARK_SCRIPT']

    s3_script_download(s3_bucket_script,input_script)
    
    # Set the environment variables for the Spark application
    spark_submit(s3_bucket_script,input_script, event)
   
