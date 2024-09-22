import tempfile
from datetime import datetime, timezone

import boto3
from pyspark.sql import SparkSession

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


def task_4_1_transformation(connection_properties, jdbc_url, bucket_name, prefix):
    spark = SparkSession.builder.appName("task4_1").getOrCreate()

    # Read data from S3
    spark_df = read_data(spark, bucket_name, prefix)

    # Filter the DataFrame for active employees
    spark_df = spark_df.filter(spark_df.status == "ACTIVE")

    # Group by designation and count
    active_counts_df = spark_df.groupBy("designation").count().withColumnRenamed("count", "active_count")

    # Join the count DataFrame with the original DataFrame on 'designation'
    spark_df = spark_df.join(active_counts_df, "designation", "left")

    # Select only the required columns
    spark_df = spark_df.select("emp_id", "designation", "active_count")

    spark_df.write \
        .mode("overwrite") \
        .jdbc(url=jdbc_url, table="ac_employee_ts_table", properties=connection_properties)
