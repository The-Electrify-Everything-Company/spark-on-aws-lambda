import boto3
import json
import os
import logging
import time
import urllib.request
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize boto3 clients with proper configuration
# Use boto3.Session for better credential management
session = boto3.session.Session()
aws_region = session.region_name

# Initialize clients using the session
s3_client = session.client('s3')
sqs_client = session.client('sqs')

# Define retry parameters for iceberg write
WRITE_MAX_RETIRES = 5
BACKOFF_FACTOR = 2
# Environment variables
DATABASE_NAME = os.environ.get("GLUE_DATABASE")
TABLE_NAME = os.environ.get("ICEBERG_TABLE")
WHATSAPP_ACCESS_TOKEN = os.environ.get('WHATSAPP_ACCESS_TOKEN')
S3_BUCKET = os.environ.get('S3_BUCKET')
S3_IMAGE_PREFIX = os.environ.get('S3_IMAGE_PREFIX')
ICEBERG_TABLE_LOCATION = os.environ.get('ICEBERG_TABLE_LOCATION') 
QUEUE_URL = os.environ.get('SQS_QUEUE_URL')

spark_session = None


# Define schema matching your table structure

ICEBERG_TABLE_SCHEMA = StructType([
    StructField("phonenumber", StringType(), True),
    StructField("recipientphonenumber", StringType(), True),
    StructField("message_id", StringType(), True),
    StructField("text_content", StringType(), True),
    StructField("direction", StringType(), True),
    StructField("type", StringType(), True),
    StructField("status", StringType(), True),
    StructField("conversation_id", StringType(), True),
    StructField("conversation_type", StringType(), True),
    StructField("pricing_model", StringType(), True),
    StructField("pricing_category", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("image_url", StringType(), True),
    StructField("image_mime_type", StringType(), True),
    StructField("image_sha256", StringType(), True),
    StructField("s3_image_path", StringType(), True)
])

def get_spark():
    global spark_session
    if spark_session is None:
        spark_session = create_iceberg_spark_session()
    return spark_session


def create_iceberg_spark_session():
    """Create Spark session optimized for Lambda with Iceberg support"""

    logger.info(f"Creating Spark session with Iceberg configuration...")
    time.sleep(2)
    spark = SparkSession.builder \
        .appName("Spark-on-AWS-Lambda") \
        .master("local[*]") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.driver.memory", "512m") \
        .config("spark.executor.memory", "1g") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.python.worker.reuse", "false") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.glue.region", aws_region) \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.warehouse", ICEBERG_TABLE_LOCATION ) \
        .config("spark.sql.defaultCatalog", "glue_catalog" ) \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.iceberg:iceberg-aws-bundle:1.4.2") \
        .config("spark.sql.catalog.glue_catalog.glue.skip-name-validation", True) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider") \
        .config("spark.sql.catalog.glue_catalog.lock-impl","org.apache.iceberg.aws.dynamodb.DynamoDbLockManager") \
        .config("spark.sql.catalog.glue_catalog.lock.table", "iceberg_lock_table") \
        .config("spark.sql.catalog.glue_catalog.commit.retry.num-retries", "3") \
        .config("spark.sql.catalog.glue_catalog.commit.retry.min-wait-ms", "2000") \
        .getOrCreate()

    logger.info("Spark session created successfully")
    return spark


def write_to_iceberg(records: list, spark):
    """
    Appends records to Iceberg table using Spark DataFrame operations
    """
    if not records:
        logger.info("No records to write")
        return
    
    logger.info(f"📊 Processing {len(records)} records to write to Iceberg table {TABLE_NAME}")
    
    # Create DataFrame from records
    df = spark.createDataFrame(records, ICEBERG_TABLE_SCHEMA)
    
    # Log the data we're about to write
    logger.info(f"Created DataFrame with {df.count()} rows and {len(df.columns)} columns")
    logger.info("Sample of data to be written:")
    df.show(3, truncate=False)
    
    # Full table name for Spark catalog
    full_table_name = f"glue_catalog.`{DATABASE_NAME}`.{TABLE_NAME}"
    
    # # Retry loop for write to iceberg
    # for attempt in range(WRITE_MAX_RETIRES):
    #     try:
    #         logger.info(f"Writing to Iceberg table: {full_table_name} (Attempt {attempt + 1}/{WRITE_MAX_RETIRES})")
            
    #         # Write DataFrame to Iceberg table
    #         df.write \
    #             .format("iceberg") \
    #             .mode("append") \
    #             .save(full_table_name)
            
    #         logger.info(f"✅ Successfully wrote {len(records)} records to Iceberg table {TABLE_NAME}")
            
    #         # Verify the write by checking the table count
    #         try:
    #             result = spark.sql(f"SELECT COUNT(*) as total_records FROM {full_table_name}")
    #             total_count = result.collect()[0]['total_records']
    #             logger.info(f"📊 Total records in {TABLE_NAME} after write: {total_count}")
    #         except Exception as e:
    #             logger.warning(f"Could not verify table count: {e}")
            
    #         return  # Exit the function on success

    #     except Exception as e:
    #         error_message = str(e)
    #         if "ICEBERG_COMMIT_ERROR" in error_message and attempt < WRITE_MAX_RETIRES - 1:
    #             wait_time = BACKOFF_FACTOR ** attempt
    #             logger.warning(f"Commit error detected. Retrying in {wait_time} seconds... (Attempt {attempt + 1})")
    #             time.sleep(wait_time)
    #         else:
    #             # This handles all other errors 
    #             logger.error(f"Fatal error writing to Iceberg table after {attempt + 1} attempts: {e}", exc_info=True)
    #             raise
   
    try:
        logger.info(f"Writing to Iceberg table: {full_table_name}")
        
        # Write DataFrame to Iceberg table
        df.write \
            .format("iceberg") \
            .mode("append") \
            .save(full_table_name)
        
        logger.info(f"✅ Successfully wrote {len(records)} records to Iceberg table {TABLE_NAME}")
        
        # Verify the write by checking the table count
        try:
            result = spark.sql(f"SELECT COUNT(*) as total_records FROM {full_table_name}")
            total_count = result.collect()[0]['total_records']
            logger.info(f"📊 Total records in {TABLE_NAME} after write: {total_count}")
        except Exception as e:
            logger.warning(f"Could not verify table count: {e}")
        
        return  # Exit the function on success

    except Exception as e:
        error_message = str(e)
        logger.error(f"There was an error write to Iceberg: {error_message}")
        raise


def download_whatsapp_media(media_id):
    """
    Download media from WhatsApp Business API
    """
    try:
        if not WHATSAPP_ACCESS_TOKEN:
            logger.error("WHATSAPP_ACCESS_TOKEN environment variable not set")
            return None

        # Get media URL
        media_url = f"https://graph.facebook.com/v17.0/{media_id}"
        headers = {
            'Authorization': f'Bearer {WHATSAPP_ACCESS_TOKEN}'
        }
        
        logger.info(f"Fetching media URL for {media_id}")
        req = urllib.request.Request(media_url, headers=headers)
        response = urllib.request.urlopen(req)
        
        media_info = json.loads(response.read().decode())
        download_url = media_info.get('url')
        
        if not download_url:
            logger.error(f"No download URL in media response: {media_info}")
            return None

        # Download actual media
        logger.info(f"Downloading media from {download_url}")
        download_req = urllib.request.Request(download_url, headers=headers)
        download_response = urllib.request.urlopen(download_req)
        
        return download_response.read()

    except urllib.request.HTTPError as e:
        logger.error(f"HTTP Error downloading media {media_id}: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error downloading media {media_id}: {str(e)}")
        return None


def upload_image_to_s3(image_data, image_id, from_number, mime_type):
    """
    Upload image to S3 bucket and return both S3 path and URL
    """
    try:
        if not S3_BUCKET:
            logger.error("S3_BUCKET environment variable not set")
            return None, None

        # Determine file extension from mime type
        extension = '.jpg'  # default
        if 'png' in mime_type:
            extension = '.png'
        elif 'gif' in mime_type:
            extension = '.gif'
        elif 'webp' in mime_type:
            extension = '.webp'

        # Create S3 key (path)
        timestamp = int(time.time())
        s3_key = f"{S3_IMAGE_PREFIX}{from_number}/{timestamp}_{image_id}{extension}"
        
        # Upload to S3
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=image_data,
            ContentType=mime_type,
            Metadata={
                'whatsapp_media_id': image_id,
                'user_phone': from_number,
                'upload_timestamp': str(timestamp)
            }
        )
        
        # Generate public URL (adjust if using private buckets)
        s3_url = f"https://{S3_BUCKET}.s3.amazonaws.com/{s3_key}"
        logger.info(f"Successfully uploaded image to S3: {s3_url}")
        
        return s3_url, s3_key  # Return both URL and path

    except Exception as e:
        logger.error(f"Error uploading image to S3: {str(e)}")
        return None, None


def handle_image_message(message, display_phone_number):
    """
    Handle inbound image messages - download from WhatsApp and upload to S3
    """
    try:
        from_number = message["from"]
        message_id = message["id"]
        timestamp = int(message["timestamp"])
        image_data = message["image"]
        media_id = image_data["id"]
        mime_type = image_data["mime_type"]
        sha256_hash = image_data["sha256"]

        logger.info(f"📸 Inbound image message from {from_number}, media_id: {media_id}")

        # Download image from WhatsApp
        image_bytes = download_whatsapp_media(media_id)
        if not image_bytes:
            logger.error(f"Failed to download image {media_id}")
            # Still record the message but without image URL and S3 path
            return {
                "phonenumber": from_number,
                "recipientphonenumber": display_phone_number,
                "message_id": message_id,
                "timestamp": timestamp,
                "direction": "inbound",
                "type": "image",
                "image_mime_type": mime_type,
                "image_sha256": sha256_hash,
                "image_url": None,
                "s3_image_path": None,
                "text_content": None,
                "status": None,
                "conversation_id": None,
                "conversation_type": None,
                "pricing_model": None,
                "pricing_category": None
            }

        # Upload to S3 - now getting both URL and path
        s3_url, s3_path = upload_image_to_s3(image_bytes, media_id, from_number, mime_type)

        return {
            "phonenumber": from_number,
            "recipientphonenumber": display_phone_number,
            "message_id": message_id,
            "timestamp": timestamp,
            "direction": "inbound",
            "type": "image",
            "image_url": s3_url,
            "image_mime_type": mime_type,
            "image_sha256": sha256_hash,
            "s3_image_path": s3_path,
            "text_content": None,
            "status": None,
            "conversation_id": None,
            "conversation_type": None,
            "pricing_model": None,
            "pricing_category": None
        }

    except Exception as e:
        logger.error(f"Error handling image message: {str(e)}")
        return None


def detect_vendor(payload):
    """
    Determines which webhook provider a payload came from, based purely on
    body shape (the SQS message body is only the inner webhook JSON - the
    original request headers are not available at this point).
    """
    if not isinstance(payload, dict):
        return 'unknown'
    if payload.get('object') == 'whatsapp_business_account':
        return 'meta'
    if payload.get('version') == 2 and payload.get('event') and isinstance(payload.get('data'), dict):
        return 'spoki'
    return 'unknown'


def build_meta_records(payload):
    """
    Parses a Meta/WhatsApp Cloud API webhook payload into a list of records
    matching ICEBERG_TABLE_SCHEMA.
    """
    records = []
    if 'object' in payload and payload['object'] == 'whatsapp_business_account':
        for entry in payload.get('entry', []):
            for change in entry.get('changes', []):
                if change['field'] == 'messages':
                    value = change.get("value", {})
                    metadata = value.get("metadata", {})
                    display_phone_number = metadata.get('display_phone_number')

                    # Handle inbound messages
                    for message in value.get("messages", []):
                        message_type = message["type"]
                        match message_type:
                            case "text":
                                from_number = message["from"]
                                text_content = message["text"]["body"]
                                message_id = message["id"]
                                timestamp = int(message["timestamp"])

                                logger.info(f"📩 Inbound message from {from_number}: {text_content}")

                                records.append({
                                    "phonenumber": from_number,
                                    "recipientphonenumber": display_phone_number,
                                    "message_id": message_id,
                                    "timestamp": timestamp,
                                    "text_content": text_content,
                                    "direction": "inbound",
                                    "type": "text",
                                    "status": None,
                                    "conversation_id": None,
                                    "conversation_type": None,
                                    "pricing_model": None,
                                    "pricing_category": None,
                                    "image_url": None,
                                    "image_mime_type": None,
                                    "image_sha256": None,
                                    "s3_image_path": None
                                })

                            case "image":
                                # Handle image messages
                                image_record = handle_image_message(message, display_phone_number)
                                if image_record:
                                    # Ensure all fields are present
                                    for field in ["status", "conversation_id", "conversation_type",
                                                "pricing_model", "pricing_category", "text_content"]:
                                        if field not in image_record:
                                            image_record[field] = None
                                    records.append(image_record)

                            # Add more message types here as needed
                            # case "audio":
                            # case "video":
                            # case "document":

                    # Handle outbound message statuses
                    for status in value.get("statuses", []):
                        message_id = status["id"]
                        status_value = status["status"]
                        recipient_id = status.get("recipient_id")
                        timestamp = int(status["timestamp"])

                        logger.info(f"📤 Outbound message {message_id} to {recipient_id} has status {status_value}")

                        record = {
                            "phonenumber": display_phone_number,
                            "recipientphonenumber": recipient_id,
                            "message_id": message_id,
                            "timestamp": timestamp,
                            "status": status_value,
                            "direction": "outbound",
                            "type": None,
                            "text_content": None,
                            "conversation_id": None,
                            "conversation_type": None,
                            "pricing_model": None,
                            "pricing_category": None,
                            "image_url": None,
                            "image_mime_type": None,
                            "image_sha256": None,
                            "s3_image_path": None
                        }

                        if "conversation" in status:
                            record["conversation_id"] = status["conversation"]["id"]
                            record["conversation_type"] = status["conversation"]["origin"].get("type")

                        if "pricing" in status:
                            record["pricing_model"] = status["pricing"].get("pricing_model")
                            record["pricing_category"] = status["pricing"].get("category")

                        records.append(record)

    return records


def _extract_spoki_timestamp(payload, data):
    """
    Spoki stores the message-creation epoch-ms timestamp under different keys
    depending on direction: "timestamp_ms" for inbound, "timestamp" for
    outbound (distinct from the top-level payload "timestamp", which is
    always a float epoch-seconds webhook-emission time).
    """
    timestamp_ms = data.get('timestamp_ms')
    if timestamp_ms is None:
        candidate = data.get('timestamp')
        if isinstance(candidate, (int, float)) and candidate > 10**12:
            timestamp_ms = candidate
    if timestamp_ms is not None:
        return int(timestamp_ms / 1000)
    return int(payload.get('timestamp', 0))


def build_spoki_records(payload):
    """
    Parses a Spoki webhook payload into a list of records matching
    ICEBERG_TABLE_SCHEMA. Only "message.inbound"/"message.outbound" text
    messages are fully handled for now; any other content_type still yields
    a minimal record with a warning logged so we can extend this once a real
    sample shows up.
    """
    records = []
    event = payload.get('event')
    if event not in ('message.inbound', 'message.outbound'):
        logger.warning(f"[Spoki] Unhandled event type '{event}', skipping")
        return records

    data = payload.get('data')
    from_phone = data.get('from_phone')
    to_phone = data.get('to_phone')
    message_id = data.get('uuid')
    direction = (data.get('direction')).lower()
    content_type = (data.get('content_type')).lower()
    timestamp = _extract_spoki_timestamp(payload, data)

    record = {
        "phonenumber": from_phone,
        "recipientphonenumber": to_phone,
        "message_id": message_id,
        "timestamp": timestamp,
        "text_content": None,
        "direction": direction,
        "type": content_type,
        "status": None,
        "conversation_id": None,
        "conversation_type": None,
        "pricing_model": None,
        "pricing_category": None,
        "image_url": None,
        "image_mime_type": None,
        "image_sha256": None,
        "s3_image_path": None
    }

    match direction:
        case "inbound":
            logger.info(f"📩 [Spoki] Inbound {content_type} message from {from_phone}")
        case "outbound":
            send_status = data.get('send_status')
            if send_status and send_status != "---":
                record["status"] = send_status.lower()
            logger.info(f"📤 [Spoki] Outbound {content_type} message from {from_phone} to {to_phone}, status={record['status']}")
        case _:
            logger.warning(f"[Spoki] Unrecognized direction '{direction}' for message {message_id}")

    if content_type == "text":
        record["text_content"] = data.get('text')
    else:
        logger.warning(f"[Spoki] Unhandled content_type '{content_type}' for message {message_id}; recording metadata only")

    records.append(record)
    return records


def handle_message_event(payload, spark):
    """
    Handles inbound webhook notifications (Meta or Spoki) and stores them in
    Iceberg. Updated to use Spark instead of Athena.
    """
    try:
        vendor = detect_vendor(payload)
        if vendor == 'meta':
            records = build_meta_records(payload)
        elif vendor == 'spoki':
            records = build_spoki_records(payload)
        else:
            logger.warning(f"Unrecognized webhook payload shape, skipping. Top-level keys: {list(payload.keys()) if isinstance(payload, dict) else type(payload)}")
            records = []

        if records:
            logger.info(f"Collected {len(records)} records to write to Iceberg")
            # Use Spark to write to Iceberg instead of Athena
            write_to_iceberg(records, spark)

        return {
            "statusCode": 200,
            "body": json.dumps({"status": "ok", "records_processed": len(records)})
        }

    except Exception as e:
        logger.error(f"Error processing POST request or writing to Iceberg: {e}", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Internal server error"})
        }


def main(event):
    try:
        logger.info(f"Event: {event}")
        start_time = time.time()
        # Extracting the WhatsApp Message Payload from SQS
        # SQS event has Records array with each record having body
        for record in event['Records']:
            payload = json.loads(record['body'])
            receipt_handle = record['receiptHandle']
            
            # Create Spark session
            spark = get_spark()
            logger.info(f"Spark Session created")
            
            # Process the event with Spark
            result = handle_message_event(payload, spark)
            logger.info(f"Processed event: {result}")
            # Delete the message from the queue using the receipt handle
            if QUEUE_URL:
                try:
                    sqs_client.delete_message(
                        QueueUrl=QUEUE_URL,
                        ReceiptHandle=receipt_handle
                    )
                    logger.info(f"Successfully deleted message from SQS queue")
                except Exception as e:
                    logger.error(f"Failed to delete message from SQS: {str(e)}")
            else:
                logger.warning("QUEUE_URL not set, skipping SQS message deletion")
        elapsed_time = time.time()- start_time
        logger.info(f"Total processing time: {elapsed_time} seconds")
        # Stop Spark session after processing all records
        spark.stop()
        
        
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Processing completed successfully"})
        }
        
    except Exception as e:
        logger.error(f"Error processing log event: {str(e)}", exc_info=True)
        
        # Ensure Spark session is stopped even on error
        try:
            if spark_session:
                spark_session.stop()
        except:
            raise


if __name__ == '__main__':
    logger.info("WhatsApp API Webhook Iceberg Write Started")
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-file", help="event data from lambda")
    args = parser.parse_args()
    
    if args.event_file:
        with open(args.event_file) as f:
            event = json.load(f)
        logger.info(f"Event for SQS: {event}")
        main(event)