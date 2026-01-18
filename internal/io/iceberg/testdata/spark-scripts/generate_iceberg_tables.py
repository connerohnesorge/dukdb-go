#!/usr/bin/env python3
"""
Generate Iceberg tables using Apache Spark for compatibility testing.
This script creates various Iceberg tables to test dukdb-go's Iceberg implementation.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime, timedelta
import sys

def create_spark_session():
    """Create Spark session with Iceberg support."""
    return (SparkSession.builder
        .appName("IcebergTableGenerator")
        .config("spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .config("spark.sql.catalog.local",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse",
                "/opt/spark-warehouse")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate())

def create_simple_table(spark, location):
    """Create a simple Iceberg table with basic data types."""
    print(f"Creating simple table at {location}")

    schema = StructType([
        StructField("id", LongType(), False),
        StructField("name", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("created_at", TimestampType(), True)
    ])

    data = [
        (1, "Alice", 100.5, datetime(2024, 1, 1, 10, 0, 0)),
        (2, "Bob", 200.75, datetime(2024, 1, 2, 11, 0, 0)),
        (3, "Charlie", 300.25, datetime(2024, 1, 3, 12, 0, 0)),
        (4, None, 400.0, datetime(2024, 1, 4, 13, 0, 0)),
        (5, "Eve", None, datetime(2024, 1, 5, 14, 0, 0))
    ]

    df = spark.createDataFrame(data, schema)
    df.writeTo(f"local.{location}").using("iceberg").create()
    print(f"✓ Created simple table with {df.count()} rows")

def create_partitioned_table(spark, location):
    """Create a partitioned Iceberg table."""
    print(f"Creating partitioned table at {location}")

    schema = StructType([
        StructField("id", LongType(), False),
        StructField("date", DateType(), False),
        StructField("category", StringType(), True),
        StructField("amount", DoubleType(), True)
    ])

    base_date = datetime(2024, 1, 1).date()
    data = []
    for i in range(100):
        date = base_date + timedelta(days=i % 10)
        category = ["A", "B", "C"][i % 3]
        data.append((i, date, category, float(i * 10.5)))

    df = spark.createDataFrame(data, schema)
    df.writeTo(f"local.{location}").using("iceberg") \
        .partitionedBy("date", "category").create()
    print(f"✓ Created partitioned table with {df.count()} rows")

def create_schema_evolution_table(spark, location):
    """Create a table and evolve its schema."""
    print(f"Creating schema evolution table at {location}")

    # Initial schema
    schema_v1 = StructType([
        StructField("id", LongType(), False),
        StructField("name", StringType(), True)
    ])

    data_v1 = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
    df_v1 = spark.createDataFrame(data_v1, schema_v1)
    df_v1.writeTo(f"local.{location}").using("iceberg").create()
    print(f"✓ Created initial version with {df_v1.count()} rows")

    # Add a new column
    spark.sql(f"ALTER TABLE local.{location} ADD COLUMN age INT")

    # Add more data with the new column
    schema_v2 = StructType([
        StructField("id", LongType(), False),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])

    data_v2 = [(4, "David", 30), (5, "Eve", 25)]
    df_v2 = spark.createDataFrame(data_v2, schema_v2)
    df_v2.writeTo(f"local.{location}").append()
    print(f"✓ Evolved schema and added 2 more rows")

def create_delete_files_table(spark, location):
    """Create a table with delete files (for testing delete support)."""
    print(f"Creating table with deletes at {location}")

    schema = StructType([
        StructField("id", LongType(), False),
        StructField("data", StringType(), True)
    ])

    # Initial data
    data = [(i, f"row_{i}") for i in range(1, 21)]
    df = spark.createDataFrame(data, schema)
    df.writeTo(f"local.{location}").using("iceberg").create()
    print(f"✓ Created table with {df.count()} rows")

    # Delete some rows (creates delete files)
    spark.sql(f"DELETE FROM local.{location} WHERE id IN (5, 10, 15)")
    print("✓ Deleted 3 rows (created delete files)")

    # Verify
    remaining = spark.sql(f"SELECT COUNT(*) as count FROM local.{location}").collect()[0][0]
    print(f"✓ Remaining rows: {remaining}")

def create_time_travel_table(spark, location):
    """Create a table with multiple snapshots for time travel testing."""
    print(f"Creating time travel table at {location}")

    schema = StructType([
        StructField("id", LongType(), False),
        StructField("value", IntegerType(), True)
    ])

    # Snapshot 1
    data1 = [(1, 100), (2, 200)]
    df1 = spark.createDataFrame(data1, schema)
    df1.writeTo(f"local.{location}").using("iceberg").create()
    print("✓ Created snapshot 1 with 2 rows")

    # Snapshot 2
    data2 = [(3, 300), (4, 400)]
    df2 = spark.createDataFrame(data2, schema)
    df2.writeTo(f"local.{location}").append()
    print("✓ Created snapshot 2 with 2 more rows")

    # Snapshot 3
    spark.sql(f"UPDATE local.{location} SET value = value * 2 WHERE id = 1")
    print("✓ Created snapshot 3 with update")

    # Show snapshot history
    snapshots = spark.sql(f"SELECT * FROM local.{location}.snapshots").collect()
    print(f"✓ Total snapshots: {len(snapshots)}")

def main():
    print("=== Iceberg Table Generator (Spark) ===\n")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Create various test tables
        create_simple_table(spark, "spark_simple")
        print()

        create_partitioned_table(spark, "spark_partitioned")
        print()

        create_schema_evolution_table(spark, "spark_schema_evolution")
        print()

        create_delete_files_table(spark, "spark_deletes")
        print()

        create_time_travel_table(spark, "spark_time_travel")
        print()

        # Also create tables in S3 (MinIO)
        print("Creating tables in MinIO S3...")
        create_simple_table(spark, "s3a://iceberg-test/spark_simple_s3")
        create_partitioned_table(spark, "s3a://iceberg-test/spark_partitioned_s3")
        print()

        print("✅ All Spark Iceberg tables created successfully!")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
