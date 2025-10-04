import json
import logging
import os
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from dataclasses import dataclass
from typing import Optional, Dict, List
import hashlib
from pathlib import Path

@dataclass
class Record:
    type: str 
    SN: str = "" 
    CID: Optional[int] = None  # Only for "record"
    source: str = ""
    destination: str = ""
    metadata: Dict[str, str] = None
    actor: str = ""  
    action: str = ""
    urids: Optional[List[str]] = None  # Only for "curated"
    total_energy: Optional[float] = None  # Only for "curated"
    date: Optional[str] = None # Only for "curated"
    # Auto-generated fields
    urid: str = "" # Will be computed in __post_init__
    created_at: float = 0
    record_hash: str = ""  # Will be computed in __post_init__

    def __post_init__(self):
        """Compute recordhash from SN, CID (or urids if curated) + createdat."""
        hash_data = {
            "sn"    : self.SN,
        }

        if self.type == "record":
            self.urid = f"{self.SN}-{self.CID}"
            hash_data.update({ "CID": self.CID, "urid": self.urid})
        elif self.type == "curated":
            self.urid = f"{self.SN}-{self.date}"
            hash_data.update({"date": self.date, "total_energy": self.total_energy})

        # Convert to JSON string and compute SHA-256 hash
        hash_str = json.dumps(hash_data, sort_keys=True).encode('utf-8')
        self.record_hash = hashlib.sha512(hash_str).digest()[:16].hex()  # First 16 bytes of SHA-512 hash

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

def batch_write_records(records: list[Record], table: str, spark):
    logger.info(f"called batch write on table ::: {table}")
    if not records:
        return
    
    if table == RC_TABLE_NAME:
        schema = StructType([
            StructField("type", StringType(), nullable=False),
            StructField("SN", StringType(), nullable=True),
            StructField("CID", IntegerType(), nullable=False),
            StructField("source", StringType(), nullable=True),
            StructField("destination", StringType(), nullable=True),
            StructField("metadata", MapType(StringType(), StringType()), nullable=True),
            StructField("actor", StringType(), nullable=True),
            StructField("action", StringType(), nullable=True),
            StructField("urid", StringType(), nullable=True),
            StructField("created_at", FloatType(), nullable=True),
            StructField("record_hash", StringType(), nullable=True)
        ])
    
    elif table == CR_TABLE_NAME:
        schema = StructType([
            StructField("type", StringType(), nullable=False),
            StructField("SN", StringType(), nullable=True),
            StructField("source", StringType(), nullable=True),
            StructField("destination", StringType(), nullable=True),
            StructField("metadata", MapType(StringType(), StringType()), nullable=True),
            StructField("actor", StringType(), nullable=True),
            StructField("action", StringType(), nullable=True),
            StructField("urids", ArrayType(StringType()), nullable=True),  
            StructField("total_energy", FloatType(), nullable=True), 
            StructField("date", StringType(), nullable=True),
            StructField("urid", StringType(), nullable=True),
            StructField("created_at", FloatType(), nullable=True),
            StructField("record_hash", StringType(), nullable=True)
        ])

    # Create DataFrame
    df = spark.createDataFrame(records, schema=schema)
    logger.info("SHOW TABLES")

    # Write DataFrame to Iceberg table
    df.writeTo(f"glue_catalog.`{DB_NAME}`.`{table}`") \
        .using("iceberg") \
            .tableProperty("commit.retry.num-retries", "10") \
                .tableProperty("commit.retry.min-wait-ms", "1000") \
                    .tableProperty("write.merge.isolation-level", "snapshot") \
                        .append()
    logger.info(f"log lineage success :::: table {table} : count : {len(records)}")

def create_iceberg_spark_session():
    """Create Spark session optimized for Lambda with Iceberg support"""

    aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    session_token = os.environ['AWS_SESSION_TOKEN']
    aws_region = os.environ['AWS_REGION']

    logger.info(f"Creating Spark session with Iceberg configuration...")

    # spark = SparkSession.builder \
    #     .appName("Spark-on-AWS-Lambda") \
    #     .master("local[*]") \
    #     .config("spark.driver.bindAddress", "127.0.0.1") \
    #     .config("spark.driver.host", "127.0.0.1") \
    #     .config("spark.driver.memory", "2g") \
    #     .config("spark.executor.memory", "2g") \
    #     .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    #     .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    #     .config("spark.hadoop.fs.s3a.session.token",session_token) \
    #     .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    #     .config("spark.sql.catalog.AwsDataCatalog", "org.apache.iceberg.spark.SparkCatalog") \
    #     .config("spark.sql.catalog.AwsDataCatalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    #     .config("spark.sql.catalog.AwsDataCatalog.warehouse", "s3://apache-iceberg-datalineage-533267385393-tst/" ) \
    #     .config("spark.sql.catalog.AwsDataCatalog.glue.region", aws_region) \
    #     .config("spark.sql.defaultCatalog", "AwsDataCatalog" ) \
    #     .getOrCreate()
    
    spark = SparkSession.builder \
        .appName("Spark-on-AWS-Lambda") \
        .master("local[*]") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.memory", "512m") \
        .config("spark.executor.memory", "512m") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.glue_catalog.glue.region", 'eu-west-2') \
        .config("spark.sql.catalog.glue_catalog.warehouse", "s3a://apache-iceberg-datalineage-533267385393-tst/" ) \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
        .config("spark.hadoop.fs.s3a.session.token", session_token) \
        .config("spark.sql.defaultCatalog", "glue_catalog" ) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider") \
        .config("spark.hadoop.hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
        .enableHiveSupport() \
        .getOrCreate()
    
        
    logger.info("Spark session created successfully")
    return spark

def main(event):
    try:
        spark = create_iceberg_spark_session()
        catalog_name = "glue_catalog"

        # Step 1: List all databases
        databases_df = spark.sql(f"SHOW DATABASES IN {catalog_name}")
        logger.info("Databases df:", databases_df)
        col_name = databases_df.columns[0]   # get the first column (whatever it's called)
        databases = [row[col_name] for row in databases_df.collect()]

        logger.info("Databases found:", databases)

        # Step 2: For each database, list tables
        for db in databases:
            # Escape the database name with backticks for Spark SQL
            db_escaped = f"`{db}`"
            logger.info(f"\nTables in database {db}:")
            tables_df = spark.sql(f"SHOW TABLES IN {catalog_name}.{db_escaped}")
            tables = [row.tableName for row in tables_df.collect()]
            logger.info(tables)

        # records = []
        # curated = []

        # for record_event in event['Records']:
        #     payload = json.loads(record_event['body'])
        #     CID = int(payload.get('CID', '0')) if str(payload.get('CID', '0')).isdigit() else 0
        #     record = Record(
        #         type=payload.get('type'),
        #         SN=payload.get('SN'),
        #         total_energy=float(payload.get('totalenergy', '0.0')),
        #         CID=CID,
        #         source=payload.get('source', ''),
        #         destination=payload.get('destination', ''),
        #         metadata=payload.get('metadata', {}),
        #         actor=payload.get('actor', ''),
        #         action=payload.get('action', ''),
        #         created_at=payload.get('created_at'),
        #         date=payload.get('date', ''),
        #         urids=payload.get('urids', [])
        #     )

        #     if record.type == "record":
        #         records.append(record)
        #     elif record.type == "curated":
        #         curated.append(record)

        # if records:
        #     batch_write_records(records,RC_TABLE_NAME,spark)
        # if curated:
        #     batch_write_records(curated, CR_TABLE_NAME,spark)
        
    except Exception as e:
        logger.error(f"Error processing log event: {str(e)}")
        # logger.error(f"Event data: {json.dumps(event, indent=2)}")
        raise

if __name__ == '__main__':

    main(None)
    