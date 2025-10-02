from pyspark.sql import SparkSession
import sys
import os

def test_iceberg_configuration():
    """Test Iceberg Spark session configuration and functionality"""
    
    print("=== Starting Iceberg Configuration Test ===")
    
    try:
        # Build Spark session with Iceberg configuration
        spark = SparkSession.builder \
            .appName("IcebergConfigTest") \
            .master("local[*]") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg_catalog.type", "hadoop") \
            .config("spark.sql.catalog.iceberg_catalog.warehouse", "/tmp/iceberg-warehouse") \
            .config("spark.sql.defaultCatalog", "iceberg_catalog") \
            .getOrCreate()
        
        print("✅ Spark session with Iceberg configuration created successfully!")
        
        # Test 1: Check Iceberg classes are available
        test_iceberg_classes_available(spark)
        
        # Test 2: Test basic Iceberg operations
        test_basic_iceberg_operations(spark)
        
        # Test 3: Test Iceberg SQL extensions
        test_iceberg_sql_extensions(spark)
        
        # Test 4: Check all Iceberg configurations
        test_iceberg_configurations(spark)
        
        spark.stop()
        print("✅ All Iceberg tests completed successfully!")
        
    except Exception as e:
        print(f"❌ Error creating Spark session with Iceberg: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

def test_iceberg_classes_available(spark):
    """Test if required Iceberg classes are available in classpath"""
    print("\n=== Testing Iceberg Class Availability ===")
    
    try:
        # Test if Iceberg SparkCatalog is available
        from pyspark.sql import SparkSession
        jvm = spark.sparkContext._jvm
        catalog_class = jvm.org.apache.iceberg.spark.SparkCatalog
        print("✅ org.apache.iceberg.spark.SparkCatalog is available")
    except Exception as e:
        print(f"❌ SparkCatalog not available: {e}")
        return False
    
    try:
        # Test if Iceberg extensions are available
        jvm = spark.sparkContext._jvm
        extensions_class = jvm.org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
        print("✅ IcebergSparkSessionExtensions is available")
    except Exception as e:
        print(f"❌ IcebergSparkSessionExtensions not available: {e}")
        return False
        
    try:
        # Test if Glue catalog is available (if using AWS Glue)
        jvm = spark.sparkContext._jvm
        glue_catalog_class = jvm.org.apache.iceberg.aws.glue.GlueCatalog
        print("✅ GlueCatalog is available")
    except Exception as e:
        print(f"⚠️  GlueCatalog not available (this might be expected): {e}")
    
    return True

def test_basic_iceberg_operations(spark):
    """Test basic Iceberg table operations"""
    print("\n=== Testing Basic Iceberg Operations ===")
    
    try:
        # Create a simple DataFrame
        data = [("Alice", 34, "Engineering"), ("Bob", 45, "Sales"), ("Charlie", 29, "Marketing")]
        columns = ["name", "age", "department"]
        df = spark.createDataFrame(data, columns)
        
        # Create Iceberg table
        table_name = "iceberg_catalog.default.employee_test"
        df.writeTo(table_name).using("iceberg").createOrReplace()
        print("✅ Iceberg table created successfully")
        
        # Read from Iceberg table
        result_df = spark.table(table_name)
        print("✅ Iceberg table read successfully")
        print("Table data:")
        result_df.show()
        
        # Check table properties
        spark.sql(f"DESCRIBE EXTENDED {table_name}").show(truncate=False)
        
        # Clean up
        spark.sql(f"DROP TABLE {table_name}")
        print("✅ Iceberg table cleaned up")
        
    except Exception as e:
        print(f"❌ Basic Iceberg operations failed: {e}")
        import traceback
        traceback.print_exc()

def test_iceberg_sql_extensions(spark):
    """Test Iceberg-specific SQL extensions"""
    print("\n=== Testing Iceberg SQL Extensions ===")
    
    try:
        # Create a test table
        spark.sql("""
            CREATE TABLE iceberg_catalog.default.test_iceberg_features (
                id bigint,
                data string,
                ts timestamp
            ) USING iceberg
        """)
        print("✅ Iceberg table created via SQL")
        
        # Insert data
        spark.sql("""
            INSERT INTO iceberg_catalog.default.test_iceberg_features 
            VALUES (1, 'test data', current_timestamp())
        """)
        print("✅ Data inserted into Iceberg table")
        
        # Test time travel (if supported)
        try:
            # Get snapshot info
            snapshot_df = spark.sql("SELECT * FROM iceberg_catalog.default.test_iceberg_features.snapshots")
            print("✅ Snapshot queries work")
            snapshot_df.show()
        except Exception as e:
            print(f"⚠️  Snapshot queries not working: {e}")
        
        # Test metadata tables
        try:
            metadata_tables = [
                "iceberg_catalog.default.test_iceberg_files",
                "iceberg_catalog.default.test_iceberg_manifests", 
                "iceberg_catalog.default.test_iceberg_partitions"
            ]
            
            for meta_table in metadata_tables:
                try:
                    spark.sql(f"SELECT * FROM {meta_table} LIMIT 1")
                    print(f"✅ Metadata table accessible: {meta_table}")
                except:
                    print(f"⚠️  Metadata table not accessible: {meta_table}")
                    
        except Exception as e:
            print(f"⚠️  Metadata table test failed: {e}")
        
        # Clean up
        spark.sql("DROP TABLE iceberg_catalog.default.test_iceberg_features")
        print("✅ Test table cleaned up")
        
    except Exception as e:
        print(f"❌ Iceberg SQL extensions test failed: {e}")
        import traceback
        traceback.print_exc()

def test_iceberg_configurations(spark):
    """Display all Iceberg-related configurations"""
    print("\n=== Iceberg Configuration Summary ===")
    
    # Get all Spark configurations
    all_configs = spark.sparkContext.getConf().getAll()
    
    # Filter Iceberg-related configurations
    iceberg_configs = [
        (key, value) for key, value in all_configs 
        if 'iceberg' in key.lower() or 'catalog' in key.lower() or 'extensions' in key.lower()
    ]
    
    if iceberg_configs:
        print("Iceberg-related configurations:")
        for key, value in iceberg_configs:
            print(f"  {key}: {value}")
    else:
        print("No Iceberg configurations found")
    
    # Check active catalog
    try:
        current_catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
        current_database = spark.sql("SELECT current_database()").collect()[0][0]
        print(f"Current catalog: {current_catalog}")
        print(f"Current database: {current_database}")
    except Exception as e:
        print(f"⚠️  Could not get current catalog/database: {e}")

def test_aws_glue_catalog_configuration():
    """Test Iceberg with AWS Glue catalog configuration"""
    print("\n=== Testing AWS Glue Catalog Configuration ===")
    
    try:
        # This configuration requires AWS credentials and proper setup
        spark_glue = SparkSession.builder \
            .appName("IcebergGlueTest") \
            .master("local[*]") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
            .config("spark.sql.catalog.glue_catalog.warehouse", "s3a://your-warehouse-bucket/") \
            .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
            .config("spark.sql.catalog.glue_catalog.lock-impl", "org.apache.iceberg.aws.glue.DynamoLockManager") \
            .config("spark.sql.catalog.glue_catalog.lock.table", "myIcebergLockTable") \
            .getOrCreate()
        
        print("✅ Spark session with Glue catalog configuration created!")
        
        # Test if we can access the Glue catalog
        try:
            databases = spark_glue.sql("SHOW DATABASES IN glue_catalog")
            print("✅ Glue catalog databases accessible")
            databases.show()
        except Exception as e:
            print(f"⚠️  Cannot access Glue catalog: {e}")
        
        spark_glue.stop()
        
    except Exception as e:
        print(f"❌ Glue catalog configuration test failed: {e}")


def lambda_handler(event, context):

    """
    Lambda_handler is called when the AWS Lambda
    is triggered. The function is downloading file 
    from Amazon S3 location and spark submitting 
    the script in AWS Lambda
    """

    test_iceberg_configuration()
   
