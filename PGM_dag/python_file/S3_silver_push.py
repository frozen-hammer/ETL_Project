from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook


def Save_to_S3(bucket_name, prefix, table_name, file_format):
    # Postgres hook to execute SQL
    postgres_hook = PostgresHook(postgres_conn_id='mypostgres')
    # SQL query
    sql_query = "SELECT * FROM {}".format(table_name)
    # Fetch data using the SQL query
    df = postgres_hook.get_pandas_df(sql_query)

    # Check if DataFrame is empty
    if df.empty:
        raise ValueError("DataFrame is empty")

    # Convert DataFrame to CSV or text format
    if file_format == 'text':
        # Convert DataFrame to text format
        data_buffer = df.to_string(index=False)
        file_extension = '.txt'
    elif file_format == 'csv':
        # Convert DataFrame to CSV format with column names included
        data_buffer = df.to_csv(index=False)
        file_extension = '.csv'
    else:
        raise ValueError("Invalid file format. Use 'csv' or 'text'.")

    # Write data to S3
    s3_hook = S3Hook(aws_conn_id='myaws')
    # Include file extension in the key
    key = prefix + table_name + file_extension
    s3_hook.load_string(
        string_data=data_buffer,
        key=key,
        bucket_name=bucket_name,
        replace=True
    )