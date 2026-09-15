"""
One-off maintenance script: removes the deprecated `write.object-storage.path`
table property from the webhook_whatsapp_api_messages Iceberg table.

That property is set on the table's own metadata (created/managed outside this
repo, directly in Glue), not by any code here - `create_iceberg_spark_session()`
in soal_whatsapp_api_iceberg_write.py never sets per-table properties. The
Iceberg runtime now hard-fails on it instead of just warning:

    java.lang.IllegalArgumentException: Property 'write.object-storage.path' has
    been deprecated and will be removed in 2.0, use 'write.data.path' instead.

Run manually, once, against the production Glue Catalog table:

    python scripts/fix_iceberg_table_properties.py

Requires the same environment variables as soal_whatsapp_api_iceberg_write.py
(GLUE_DATABASE, ICEBERG_TABLE, ICEBERG_TABLE_LOCATION) plus valid AWS
credentials with permission to alter the Glue table.
"""

import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# The production Lambda container image bundles these jars on Spark's classpath
# at build time (see download_jars.sh / Dockerfile's ICEBERG_FRAMEWORK_VERSION and
# ICEBERG_FRAMEWORK_SUB_VERSION build args), so create_iceberg_spark_session()
# itself assumes they're already present. Running this script outside that
# container needs the same jars fetched via --packages instead - set before
# import so it's in place before the JVM gateway launches.
os.environ.setdefault(
    "PYSPARK_SUBMIT_ARGS",
    "--packages "
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.0,"
    "software.amazon.awssdk:bundle:2.48.1,"
    "software.amazon.awssdk:url-connection-client:2.48.1 "
    "pyspark-shell",
)

from soal_whatsapp_api_iceberg_write import (  # noqa: E402
    DATABASE_NAME,
    TABLE_NAME,
    create_iceberg_spark_session,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

DEPRECATED_PROPERTY = "write.object-storage.path"


def show_table_properties(spark, full_table_name):
    rows = spark.sql(f"SHOW TBLPROPERTIES {full_table_name}").collect()
    props = {row["key"]: row["value"] for row in rows}
    logger.info(f"Current table properties for {full_table_name}: {props}")
    return props


def main():
    full_table_name = f"glue_catalog.`{DATABASE_NAME}`.{TABLE_NAME}"
    spark = create_iceberg_spark_session()

    try:
        before = show_table_properties(spark, full_table_name)
        if DEPRECATED_PROPERTY not in before:
            logger.info(f"'{DEPRECATED_PROPERTY}' is not set on {full_table_name}; nothing to do")
            return

        logger.info(f"Unsetting '{DEPRECATED_PROPERTY}' on {full_table_name}")
        spark.sql(
            f"ALTER TABLE {full_table_name} UNSET TBLPROPERTIES ('{DEPRECATED_PROPERTY}')"
        )

        after = show_table_properties(spark, full_table_name)
        if DEPRECATED_PROPERTY in after:
            raise RuntimeError(
                f"'{DEPRECATED_PROPERTY}' is still present after ALTER TABLE: {after}"
            )
        logger.info(f"Successfully removed '{DEPRECATED_PROPERTY}' from {full_table_name}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
