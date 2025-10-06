#!/usr/bin/env bash
FRAMEWORK=$1
SPARK_HOME=$2
HADOOP_VERSION=$3
AWS_SDK_VERSION=$4
DELTA_FRAMEWORK_VERSION=$5
HUDI_FRAMEWORK_VERSION=$6
ICEBERG_FRAMEWORK_VERSION=$7
ICEBERG_FRAMEWORK_SUB_VERSION=$8
DEEQU_FRAMEWORK_VERSION=$9

mkdir $SPARK_HOME/conf
echo "SPARK_LOCAL_IP=127.0.0.1" > $SPARK_HOME/conf/spark-env.sh
echo "JAVA_HOME=/usr/lib/jvm/$(ls /usr/lib/jvm |grep java)/jre" >> $SPARK_HOME/conf/spark-env.sh

# Download core S3 filesystem JARs with updated versions
wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar -P ${SPARK_HOME}/jars/
wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar -P ${SPARK_HOME}/jars/

# Additional JARs for better S3 compatibility
wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/${HADOOP_VERSION}/hadoop-common-${HADOOP_VERSION}.jar -P ${SPARK_HOME}/jars/
wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client/${HADOOP_VERSION}/hadoop-client-${HADOOP_VERSION}.jar -P ${SPARK_HOME}/jars/
wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/${HADOOP_VERSION}/hadoop-client-api-${HADOOP_VERSION}.jar -P ${SPARK_HOME}/jars/
wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/${HADOOP_VERSION}/hadoop-client-runtime-${HADOOP_VERSION}.jar -P ${SPARK_HOME}/jars/

# Add Hadoop statistics and fs libraries to fix NoSuchMethodError
wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-annotations/${HADOOP_VERSION}/hadoop-annotations-${HADOOP_VERSION}.jar -P ${SPARK_HOME}/jars/
wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-auth/${HADOOP_VERSION}/hadoop-auth-${HADOOP_VERSION}.jar -P ${SPARK_HOME}/jars/
wget -q https://repo1.maven.org/maven2/org/apache/hadoop/thirdparty/hadoop-shaded-guava/${HADOOP_VERSION}/hadoop-shaded-guava-${HADOOP_VERSION}.jar -P ${SPARK_HOME}/jars/
wget -q https://repo1.maven.org/maven2/org/apache/hadoop/thirdparty/hadoop-shaded-protobuf_3_7/${HADOOP_VERSION}/hadoop-shaded-protobuf_3_7-${HADOOP_VERSION}.jar -P ${SPARK_HOME}/jars/
wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-mapreduce-client-core/${HADOOP_VERSION}/hadoop-mapreduce-client-core-${HADOOP_VERSION}.jar -P ${SPARK_HOME}/jars/
wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-mapreduce-client-common/${HADOOP_VERSION}/hadoop-mapreduce-client-common-${HADOOP_VERSION}.jar -P ${SPARK_HOME}/jars/
wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-hdfs-client/${HADOOP_VERSION}/hadoop-hdfs-client-${HADOOP_VERSION}.jar -P ${SPARK_HOME}/jars/

# Add additional Hadoop libraries to fix S3A filesystem issues
wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/${HADOOP_VERSION}/hadoop-client-api-${HADOOP_VERSION}.jar -P ${SPARK_HOME}/jars/
wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/${HADOOP_VERSION}/hadoop-client-runtime-${HADOOP_VERSION}.jar -P ${SPARK_HOME}/jars/

# Fix for IOStatisticsBinding NoSuchMethodError
# Download specific version that contains the required IOStatisticsBinding class
FIXED_VERSION="3.3.4"
echo "Downloading fixed Hadoop libraries version $FIXED_VERSION"
wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/$FIXED_VERSION/hadoop-common-$FIXED_VERSION.jar -P ${SPARK_HOME}/jars/
wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/$FIXED_VERSION/hadoop-aws-$FIXED_VERSION.jar -P ${SPARK_HOME}/jars/

# Download specific statistics implementation jars
wget -q https://repo1.maven.org/maven2/org/apache/hadoop/thirdparty/hadoop-shaded-guava/1.1.1/hadoop-shaded-guava-1.1.1.jar -P ${SPARK_HOME}/jars/ || echo "hadoop-shaded-guava not found"

# Download specific fs-statistics JAR that contains IOStatisticsBinding
wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/$FIXED_VERSION/hadoop-common-$FIXED_VERSION-tests.jar -P ${SPARK_HOME}/jars/ || echo "hadoop-common-tests not found"

# Download additional S3A implementation classes
wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/$FIXED_VERSION/hadoop-aws-$FIXED_VERSION-tests.jar -P ${SPARK_HOME}/jars/ || echo "hadoop-aws-tests not found"

# Copy the existing log4j.properties file to the Spark conf directory
echo "Copying existing log4j.properties file to Spark conf directory"
cp /opt/spark-on-lambda-handler/log4j.properties ${SPARK_HOME}/conf/

# Create a core-site.xml file with S3A configurations
echo "Creating core-site.xml file"
cat > ${SPARK_HOME}/conf/core-site.xml << EOL
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>fs.s3a.impl</name>
        <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
    </property>
    <property>
        <name>fs.s3a.aws.credentials.provider</name>
        <value>com.amazonaws.auth.DefaultAWSCredentialsProviderChain</value>
    </property>
    <property>
        <name>fs.s3a.connection.maximum</name>
        <value>100</value>
    </property>
    <property>
        <name>fs.s3a.experimental.input.fadvise</name>
        <value>sequential</value>
    </property>
    <property>
        <name>fs.s3a.impl.disable.cache</name>
        <value>true</value>
    </property>
    <property>
        <name>fs.s3a.path.style.access</name>
        <value>true</value>
    </property>
    <property>
        <name>fs.s3a.committer.name</name>
        <value>directory</value>
    </property>
    <property>
        <name>fs.s3a.committer.staging.conflict-mode</name>
        <value>append</value>
    </property>
    <property>
        <name>fs.s3a.committer.staging.unique-filenames</name>
        <value>true</value>
    </property>
    <property>
        <name>fs.s3a.fast.upload</name>
        <value>true</value>
    </property>
    <property>
        <name>mapreduce.fileoutputcommitter.algorithm.version</name>
        <value>2</value>
    </property>
</configuration>
EOL

# Add AWS SDK v2components for better S3 compatibility
wget -q https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.20.56/s3-2.20.56.jar -P ${SPARK_HOME}/jars/
wget -q https://repo1.maven.org/maven2/software/amazon/awssdk/utils/2.20.56/utils-2.20.56.jar -P ${SPARK_HOME}/jars/
wget -q https://repo1.maven.org/maven2/software/amazon/awssdk/auth/2.20.56/auth-2.20.56.jar -P ${SPARK_HOME}/jars/
wget -q https://repo1.maven.org/maven2/software/amazon/awssdk/http-client-spi/2.20.56/http-client-spi-2.20.56.jar -P ${SPARK_HOME}/jars/
wget -q https://repo1.maven.org/maven2/software/amazon/awssdk/regions/2.20.56/regions-2.20.56.jar -P ${SPARK_HOME}/jars/
wget -q https://repo1.maven.org/maven2/software/amazon/awssdk/sdk-core/2.20.56/sdk-core-2.20.56.jar -P ${SPARK_HOME}/jars/
wget -q https://repo1.maven.org/maven2/software/amazon/awssdk/apache-client/2.20.56/apache-client-2.20.56.jar -P ${SPARK_HOME}/jars/
wget -q https://repo1.maven.org/maven2/software/amazon/awssdk/aws-core/2.20.56/aws-core-2.20.56.jar -P ${SPARK_HOME}/jars/

# jar files needed to conncet to Snowflake
#wget -q https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/2.12.0-spark_3.3/spark-snowflake_2.12-2.12.0-spark_3.3.jar -P ${SPARK_HOME}/jars/
#wget -q https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.13.33/snowflake-jdbc-3.13.33.jar -P ${SPARK_HOME}/jars/

echo 'Framework is:'
echo $FRAMEWORK

IFS=',' read -ra FRAMEWORKS <<< "$FRAMEWORK"

for fw in "${FRAMEWORKS[@]}"; do
echo $fw
    case "$fw" in
        HUDI)
            wget -q https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.3-bundle_2.12/${HUDI_FRAMEWORK_VERSION}/hudi-spark3.3-bundle_2.12-${HUDI_FRAMEWORK_VERSION}.jar -P ${SPARK_HOME}/jars/
            ;;
        DELTA)
            wget -q https://repo1.maven.org/maven2/io/delta/delta-core_2.12/${DELTA_FRAMEWORK_VERSION}/delta-core_2.12-${DELTA_FRAMEWORK_VERSION}.jar -P ${SPARK_HOME}/jars/
            wget -q https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_FRAMEWORK_VERSION}/delta-storage-${DELTA_FRAMEWORK_VERSION}.jar -P ${SPARK_HOME}/jars/
            ;;
        ICEBERG)
            wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${ICEBERG_FRAMEWORK_VERSION}/${ICEBERG_FRAMEWORK_SUB_VERSION}/iceberg-spark-runtime-${ICEBERG_FRAMEWORK_VERSION}-${ICEBERG_FRAMEWORK_SUB_VERSION}.jar -P ${SPARK_HOME}/jars/
            wget -q https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.20.23/bundle-2.20.23.jar -P ${SPARK_HOME}/jars/
            wget -q https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/2.20.23/url-connection-client-2.20.23.jar -P ${SPARK_HOME}/jars/
            ;;
        SNOWFLAKE)
            wget -q https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/2.12.0-spark_3.3/spark-snowflake_2.12-2.12.0-spark_3.3.jar -P ${SPARK_HOME}/jars/
            wget -q https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.13.33/snowflake-jdbc-3.13.33.jar -P ${SPARK_HOME}/jars/
            ;;
        REDSHIFT)
            wget -q https://repo1.maven.org/maven2/io/github/spark-redshift-community/spark-redshift_2.12/4.1.1/spark-redshift_2.12-4.1.1.jar -P ${SPARK_HOME}/jars/
            wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.13/3.3.0/spark-avro_2.13-3.3.0.jar  -P ${SPARK_HOME}/jars/
            wget -q https://s3.amazonaws.com/redshift-downloads/drivers/jdbc/2.1.0.18/redshift-jdbc42-2.1.0.18.zip -P ${SPARK_HOME}/jars/
            wget -q https://repo1.maven.org/maven2/com/eclipsesource/minimal-json/minimal-json/0.9.1/minimal-json-0.9.1.jar -P ${SPARK_HOME}/jars/
            # Unzip the Redshift JDBC driver
            unzip -o ${SPARK_HOME}/jars/redshift-jdbc42-2.1.0.18.zip -d ${SPARK_HOME}/jars/
            ;;
        DEEQU)
            wget -q https://repo1.maven.org/maven2/com/amazon/deequ/deequ/${DEEQU_FRAMEWORK_VERSION}/deequ-${DEEQU_FRAMEWORK_VERSION}.jar -P ${SPARK_HOME}/jars/
            ;;
        *)
            echo "Unknown framework: $fw"
            ;;
    esac
done