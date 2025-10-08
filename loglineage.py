import argparse
import time
import json
import logging
import os
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from dataclasses import dataclass
from typing import Optional, Dict, List
import hashlib

@dataclass
class Record:
    type: str 
    sn: str = "" 
    cid: Optional[int] = None  # Only for "record"
    origin: str = ""
    destination: str = ""
    metadata: Dict[str, str] = None
    actorid: str = ""  
    actionid: str = ""
    urids: Optional[List[str]] = None  # Only for "curated"
    totalenergy: Optional[float] = None  # Only for "curated"
    date: Optional[str] = None # Only for "curated"
    # Auto-generated fields
    urid: str = "" # Will be computed in __post_init__
    createdat: float = 0
    recordhash: str = ""  # Will be computed in __post_init__

    def __post_init__(self):
        """Compute recordhash from SN, CID (or urids if curated) + createdat."""
        hash_data = {
            "sn"    : self.sn,
        }

        if self.type == "record":
            self.urid = f"{self.sn}-{self.cid}"
            hash_data.update({ "CID": self.cid, "urid": self.urid})
        elif self.type == "curated":
            self.urid = f"{self.sn}-{self.date}"
            hash_data.update({"date": self.date, "total_energy": self.totalenergy})

        # Convert to JSON string and compute SHA-256 hash
        hash_str = json.dumps(hash_data, sort_keys=True).encode('utf-8')
        self.recordhash = hashlib.sha512(hash_str).digest()[:16].hex()  # First 16 bytes of SHA-512 hash

MAX_RETRIES = 2
BASE_DELAY = 1

# Initialize AWS clients
athena = boto3.client('athena')
s3 = boto3.resource('s3')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DB_NAME = os.environ['DATABASE_NAME']
RC_TABLE_NAME = os.environ['RC_TABLE_NAME']
CR_TABLE_NAME = os.environ['CR_TABLE_NAME']
ICEBERG_WORKGROUP = os.environ['ICB_WG']

# _spark = None

def batch_write_records(records: list[Record], table: str, spark):
    logger.info(f"called batch write on table ::: {table}")
    if not records:
        return
    
    if table == RC_TABLE_NAME:
        schema = StructType([
            StructField("sn", StringType(), nullable=True),
            StructField("cid", IntegerType(), nullable=False),
            StructField("origin", StringType(), nullable=True),
            StructField("destination", StringType(), nullable=True),
            StructField("metadata", MapType(StringType(), StringType()), nullable=True),
            StructField("actorid", StringType(), nullable=True),
            StructField("actionid", StringType(), nullable=True),
            StructField("urid", StringType(), nullable=True),
            StructField("createdat", FloatType(), nullable=True),
            StructField("recordhash", StringType(), nullable=True)
        ])
    
    elif table == CR_TABLE_NAME:
        schema = StructType([
            StructField("origin", StringType(), nullable=True),
            StructField("destination", StringType(), nullable=True),
            StructField("metadata", MapType(StringType(), StringType()), nullable=True),
            StructField("actorid", StringType(), nullable=True),
            StructField("actionid", StringType(), nullable=True),
            StructField("urids", ArrayType(StringType()), nullable=True),  
            StructField("totalenergy", FloatType(), nullable=True), 
            StructField("date", StringType(), nullable=True),
            StructField("urid", StringType(), nullable=True),
            StructField("createdat", FloatType(), nullable=True),
            StructField("recordhash", StringType(), nullable=True)
        ])

    # Create DataFrame
    df = spark.createDataFrame(records, schema=schema)
    if table == CR_TABLE_NAME:
        df = df.withColumnRenamed("createdat", "timestamp")

    # df.createOrReplaceTempView("tmp_df")
    # spark.sql(f"""
    #     INSERT INTO glue_catalog.`{DB_NAME}`.{table}
    #     SELECT * FROM tmp_df
    #     """)
    # Write DataFrame to Iceberg table
    df.writeTo(f"glue_catalog.`{DB_NAME}`.{table}") \
        .using("iceberg") \
            .tableProperty("commit.retry.num-retries", "50") \
                .tableProperty("commit.retry.min-wait-ms", "3000") \
                    .tableProperty("write.merge.isolation-level", "snapshot") \
                        .tableProperty("write.distribution-mode", "hash") \
                            .append()
    logger.info(f"log lineage success :::: table {table} : count : {len(records)}")


# def get_spark_session():
#     global _spark
#     if _spark is None:
#         _spark = create_iceberg_spark_session()
#     return _spark


def create_iceberg_spark_session():
    """Create Spark session optimized for Lambda with Iceberg support"""
    # aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    # aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    # session_token = os.environ['AWS_SESSION_TOKEN']
    aws_region = os.environ['AWS_REGION']

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
        .config("spark.sql.catalog.glue_catalog.warehouse", "s3://apache-iceberg-datalineage-533267385393-tst/" ) \
        .config("spark.sql.defaultCatalog", "glue_catalog" ) \
        .config("spark.sql.catalog.glue_catalog.glue.skip-name-validation", True) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider") \
        .config("spark.sql.catalog.glue_catalog.lock-impl","org.apache.iceberg.aws.dynamodb.DynamoDbLockManager") \
        .config("spark.sql.catalog.glue_catalog.lock.table", "iceberg_lock_table") \
        .config("spark.sql.catalog.glue_catalog.commit.retry.num-retries", "10") \
        .config("spark.sql.catalog.glue_catalog.commit.retry.min-wait-ms", "2000") \
        .getOrCreate()

    # spark = SparkSession.builder \
    #     .appName("Spark-on-AWS-Lambda") \
    #     .master("local[*]") \
    #     .config("spark.driver.bindAddress", "127.0.0.1") \
    #     .config("spark.driver.host", "127.0.0.1") \
    #     .config("spark.driver.memory", "512m") \
    #     .config("spark.executor.memory", "512m") \
    #     .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    #     .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    #     .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    #     .config("spark.hadoop.hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
    #     .config("spark.sql.catalog.glue_catalog.warehouse", "s3://apache-iceberg-datalineage-533267385393-tst/") \
    #     .config("spark.sql.catalog.glue_catalog.glue.region", aws_region) \
    #     .config("spark.sql.catalog.glue_catalog.glue.skip-name-validation", True) \
    #     .config("spark.sql.defaultCatalog", "glue_catalog" ) \
    # .config("spark.sql.catalog.glue_catalog.lock.table", "iceberg_lock_table") \
    #     .config("spark.sql.catalog.glue_catalog.commit.retry.num-retries", "10") \
    #     .config("spark.sql.catalog.glue_catalog.commit.retry.min-wait-ms", "2000") \
    #     .getOrCreate()
        
    logger.info("Spark session created successfully")
    return spark

def main(event):
    try:
        spark = create_iceberg_spark_session()

        records = []
        curated = []

        for record_event in event['Records']:
            payload = json.loads(record_event['body'])
            CID = int(payload.get('CID', '0')) if str(payload.get('CID', '0')).isdigit() else 0
            record = Record(
                type=payload.get('type'),
                sn=payload.get('SN'),
                totalenergy=float(payload.get('totalenergy', '0.0')),
                cid=CID,
                origin=payload.get('source', ''),
                destination=payload.get('destination', ''),
                metadata=payload.get('metadata', {}),
                actorid=payload.get('actor', ''),
                actionid=payload.get('action', ''),
                createdat=payload.get('created_at'),
                date=payload.get('date', ''),
                urids=payload.get('urids', [])
            )

            if record.type == "record":
                records.append(record)
            elif record.type == "curated":
                curated.append(record)

        if records:
            batch_write_records(records,RC_TABLE_NAME,spark)
        if curated:
            batch_write_records(curated, CR_TABLE_NAME,spark)
        spark.stop()
    except Exception as e:
        logger.error(f"Error processing log event: {str(e)}")
        try:
            spark.stop()
        except:
            pass
        # logger.error(f"Event data: {json.dumps(event, indent=2)}")
        raise

if __name__ == '__main__':

    logger.info("lineage logger started")
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-file",
                        help="event data from lambda")
    args = parser.parse_args()
    with open(args.event_file) as f:
        event = json.load(f)
    main(event)
    