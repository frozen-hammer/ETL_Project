from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
from pyspark.sql.types import LongType, DateType
from datetime import datetime, timezone
import boto3
import tempfile

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


def task_4_2_transformation(jdbc_url, bucket_name , prefix ,prefix2 ,connection_properties):

    spark = SparkSession.builder.appName("task4_2").getOrCreate()

    # Read data
    calendar_data = read_data(spark, bucket_name, prefix)
    leave_data = read_data(spark, bucket_name, prefix2)

    # Get current year and current date
    current_year = datetime.now().year
    current_date = datetime.now().date()

    #Filtering the data for current_year
    calendar_data = calendar_data.filter(year("date")==current_year)
    leave_data = leave_data.filter(year("date")==current_year)

    # Convert date columns to DateType
    to_date = lambda col: col.cast(DateType())
    leave_data = leave_data.withColumn("leave_date", to_date(col("date"))).drop("date")
    calendar_data = calendar_data.withColumn("calendar_date", to_date(col("date"))).drop("date")

    # Filter leave data of leaves which are about to come
    leave_data_filtered = leave_data.filter(col("leave_date") > current_date)

    # Filter the DataFrame to include only dates after the current date
    holidays_upcoming = calendar_data.filter(col("calendar_date") > current_date)
    holidays_upcoming = holidays_upcoming.filter(dayofweek(col("calendar_date")).isin([2, 3, 4, 5, 6]))

    # Count the rows in the filtered DataFrame
    holiday_days_count = holidays_upcoming.count()

    # Collect calendar_date values into a list
    calendar_dates = [row.calendar_date for row in holidays_upcoming.select("calendar_date").collect()]

    # Define expression for weekends 1 is Sunday and 7 is Saturday
    weekend_expr = dayofweek(col("leave_date")).isin([1, 7])

    # Filter out weekends and holidays
    leave_data_filtered = leave_data_filtered.filter(~(weekend_expr | col("leave_date").isin(calendar_dates)))
    leave_data_filtered = leave_data_filtered.filter(col("status") != "CANCELLED")

    # Remove duplicates
    leave_data_filtered = leave_data_filtered.dropDuplicates(["emp_id", "leave_date"])

    # Calculate total leaves taken by each employee
    leave_count = leave_data_filtered.groupBy("emp_id").agg(count("*").alias("upcoming_leaves"))

    # Calculate end_date for the current year
    end_date = "{}-12-31".format(current_year)

    # Calculate the number of days between current date and end date
    days_diff = spark.sql(f"SELECT datediff(to_date('{end_date}'), current_date()) + 1 as days_diff").collect()[0]['days_diff']

    # Create date range
    date_range = spark.range(0, days_diff).select(expr("date_add(current_date(), cast(id as int))").alias("date"))

    # Filter out weekends (Saturday and Sunday)
    working_days = date_range.filter(dayofweek("date").isin([2, 3, 4, 5, 6])).count()
    total_working_days = working_days - holiday_days_count

    # Calculate potential leaves for the current year
    potential_leaves_year = leave_count.withColumn("potential_leaves_percentage", (col("upcoming_leaves") / total_working_days) * 100)
    upcoming_leaves = potential_leaves_year.select("emp_id","upcoming_leaves").filter(col("potential_leaves_percentage") > 8)
    #potential_leaves_year.orderBy("emp_id").show(30)
    #upcoming_leaves.orderBy("emp_id").show(30)

    upcoming_leaves.write \
        .mode("overwrite") \
        .jdbc(url=jdbc_url, table="employee_upcoming_leaves_table" , properties=connection_properties)

# Stop SparkSession
spark.stop()