from pyspark.sql import SparkSession
from smart_open import open
import tempfile

def read_Data():
    # spark = SparkSession.builder.appName("Connect").config("spark.jars", "/home/priyanshu/Documents/postgresql-42.6.2.jar").getOrCreate()
    spark = SparkSession.builder.appName("Connect").getOrCreate()
    s3_path = 's3://ttn-de-bootcamp-2024-bronze-us-east-1/gagan.thakur/upload_from_local/games_data.csv'
    with open(s3_path, 'rb') as f:
        # Read the data from S3
        data = f.read()
        # Convert bytes to string
        data_str = data.decode('utf-8')
        # Write data to a temporary file
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as tmp_file:
            tmp_file.write(data_str)
            tmp_file_path = tmp_file.name
        # Read data from the temporary file into a Spark DataFrame
        df = spark.read.csv(tmp_file_path, header=True, inferSchema=True)

        # Define the JDBC URL and properties
        jdbc_url = "jdbc:postgresql://52.23.199.153:5432/advance"
        connection_properties = {
            "user": "priyanshu",
            "password": "1234",
            "driver": "org.postgresql.Driver"
        }

        df.write \
            .mode("append") \
            .jdbc(url=jdbc_url, table='games_played', properties=connection_properties)

read_Data()