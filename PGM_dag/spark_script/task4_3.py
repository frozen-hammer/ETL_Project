import tempfile
from datetime import datetime,timezone

import boto3
from pyspark.sql import SparkSession
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



def task_4_3_transformation(jdbc_url, connection_properties, bucket_name, prefix, prefix2, prefix3):

    spark = SparkSession.builder.appName("task4_1").getOrCreate()


    emp_leave_quota_data = read_data(spark, bucket_name, prefix)
    employee_leave_calendar_data = read_data(spark, bucket_name, prefix2)
    employee_leave_application_data = read_data(spark, bucket_name, prefix3)

    current_year = datetime.now().year
    current_date = datetime.now().date()

    # Filtering the data for current_year
    emp_leave_quota_data = emp_leave_quota_data.filter(col("year") == current_year)
    employee_leave_calendar_data = employee_leave_calendar_data.filter(year("date") == current_year)
    employee_leave_application_data = employee_leave_application_data.filter(year("date") == current_year)

    # Filter active leaves
    active_leaves = employee_leave_application_data.filter(col("status") == "ACTIVE")

    # Calculate total leaves taken by each employee and year
    total_leaves_taken = active_leaves.groupBy("emp_id", year("date").alias("year")).agg(
        count("*").alias("total_leaves_taken"))
    # print("total_leaves_taken")
    # total_leaves_taken.orderBy("emp_id").show()

    # Join total_leave_quota and total_leaves_taken
    leave_data = emp_leave_quota_data.join(total_leaves_taken, ["emp_id", "year"], "left")

    # Calculate percentage of leave quota used
    leave_data = leave_data.withColumn("percentage_used", expr("total_leaves_taken / leave_quota * 100")).filter(
        col("total_leaves_taken").isNotNull())
    # print("leave_data")
    # leave_data.orderBy("emp_id").show()

    # Identify employees whose availed leave quota exceeds 80% for each year
    employees_to_notify = leave_data.filter(col("percentage_used") > 80)
    # print("employees_to_notify")

    employees_to_notify.select("emp_id").orderBy("emp_id").show()

    final_df = employees_to_notify.select("emp_id", "year", "leave_quota", "total_leaves_taken", "percentage_used")

    # Write the final DataFrame to the database table
    final_df.write \
        .mode("overwrite") \
        .jdbc(url=jdbc_url, table="employee_leaves_spend_table", properties=connection_properties)
