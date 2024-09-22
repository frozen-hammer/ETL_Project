import tempfile
from datetime import datetime, timezone
import boto3
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *


def read_data(spark, bucket_name, prefix):
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    # Get the most recent file based on LastModified timestamp
    latest_file = None
    latest_timestamp = datetime(1970, 1, 1, tzinfo=timezone.utc)  # Initialize with UTC timezone
    for obj in response.get('Contents', []):
        obj_last_modified = obj['LastModified'].replace(tzinfo=timezone.utc)  # Make the timestamp timezone-aware
        if not obj['Key'].endswith('/') and (latest_file is None or obj_last_modified > latest_timestamp):
            latest_file = obj['Key']
            latest_timestamp = obj_last_modified

    if latest_file:
        # s3_path = f's3://{bucket_name_bronze}/{latest_file}'
        # Read the data from S3
        data_obj = s3.get_object(Bucket=bucket_name, Key=latest_file)
        data_str = data_obj['Body'].read().decode('utf-8')
        # Write data to a temporary file
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as tmp_file:
            tmp_file.write(data_str)
            tmp_file_path = tmp_file.name
        # Read data from the temporary file into a Spark DataFrame
        spark_df = spark.read.csv(tmp_file_path, header=True, inferSchema=True)

        return spark_df  # Return the Spark DataFrame




def transform_timestamp_data(connection_properties, jdbc_url, bucket_name, prefix):
    spark = SparkSession.builder.appName("task1").getOrCreate()
    spark_df = read_data(spark, bucket_name, prefix)

    # Convert timestamps to dates
    spark_df = (spark_df.withColumn("start_date_tmp", from_unixtime("start_date").cast("timestamp"))
                .withColumn("end_date_tmp", from_unixtime("end_date").cast("timestamp"))
                .withColumn("start_date", from_unixtime("start_date").cast("date"))
                .withColumn("end_date", from_unixtime("end_date").cast("date")))

    # Handle duplicates by keeping the record with the highest salary and corresponding designation
    windowSpec = Window.partitionBy("emp_id", "start_date", "end_date").orderBy(desc("salary"))
    spark_df = spark_df.withColumn("row_number", row_number().over(windowSpec)) \
        .filter(col("row_number") == 1) \
        .drop("row_number")

    # Mark ongoing designations as "ACTIVE" and others as "INACTIVE"
    spark_df = spark_df.withColumn("status", when(col("end_date").isNull(), "ACTIVE").otherwise("INACTIVE"))
    # df.show()
    spark_df.printSchema()
    # Ensure continuity between records for the same employee
    windowSpec = Window.partitionBy("emp_id").orderBy("start_date")
    spark_df = spark_df.withColumn("previous_end_date", lag("end_date").over(windowSpec))
    spark_df = spark_df.withColumn("start_date", when(col("previous_end_date").isNull(), col("start_date")).otherwise(
        col("previous_end_date")))
    spark_df = spark_df.drop("previous_end_date")

    # Sort the DataFrame by emp_id to group records for each emp_id together
    spark_df = spark_df.orderBy("emp_id", "start_date_tmp").drop("start_date_tmp", "end_date_tmp")
    print("sdohndsfodsnfidsffjndsvjdsbvjdsvbdsjvdsv")

    spark_df.write \
        .mode("overwrite") \
        .jdbc(url=jdbc_url, table="staging_employee_ts_table", properties=connection_properties)
