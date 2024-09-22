import tempfile
from datetime import datetime, timezone
import boto3
from pyspark.sql import SparkSession


def read_data(jdbc_url, connection_properties, bucket_name, prefix, table_name):
    spark = SparkSession.builder.appName("task1").getOrCreate()
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

        # Write Spark DataFrame to PostgreSQL table
        spark_df.write \
            .jdbc(url=jdbc_url,
                  table=table_name,
                  mode="append",  # or "overwrite" depending on your requirement
                  properties=connection_properties)

