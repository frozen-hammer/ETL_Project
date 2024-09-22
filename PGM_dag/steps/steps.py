# Define global variables
jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
connection_properties = {
    "user": "priyanshu",
    "password": "1234",
    "driver": "org.postgresql.Driver",
    "batchsize": "50000"  # Set the batch size to 50000 records
}
bucket_name_bronze = 'ttn-de-bootcamp-2024-bronze-us-east-1'
bucket_name_silver = 'ttn-de-bootcamp-2024-silver-us-east-1'
ED = 'madhav.kashyap/Project/Employee_Data'
ELD = 'madhav.kashyap/Project/Employee_leave_data/'
ELQD = 'madhav.kashyap/Project/Employee_leave_quota_data/'
ELCD = 'madhav.kashyap/Project/Employee_Leave_Calender_Data/'
ELTF = 'madhav.kashyap/Project/Employee_timeframe_data/'
ELSD = 'madhav.kashyap/Project/Employee_leaves_spend_data/'


# Define the EMR step
# jdbc_url, connection_properties, bucket_name, prefix, table_name
task1_step = [
    {
        'Name': 'Task 1 Step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--jars', 's3://your-bucket/jars/postgresql-42.2.23.jar',
                's3://ttn-de-bootcamp-2024-bronze-us-east-1/priyanshu.rana/spark-scripts/script.py',
                '--jdbc_url', jdbc_url,
                '--connection_properties', connection_properties,
                '--bucket_name', bucket_name_bronze,
                '--prefix', ED
            ],
        },
    }
]

# connection_properties, jdbc_url, bucket_name, prefix
task2_step = [
    {
        'Name': 'Task 2 Step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--jars', 's3://your-bucket/jars/postgresql-42.2.23.jar',
                's3://ttn-de-bootcamp-2024-bronze-us-east-1/priyanshu.rana/spark-scripts/script.py',
                '--jdbc_url', jdbc_url,
                '--connection_properties', connection_properties,
                '--bucket_name', bucket_name_bronze,
                '--prefix', ELTF
            ],
        },
    }
]

# connection_properties, jdbc_url, bucket_name, prefix
task4_1_step = [
    {
        'Name': 'Task 2 Step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--jars', 's3://your-bucket/jars/postgresql-42.2.23.jar',
                's3://ttn-de-bootcamp-2024-bronze-us-east-1/priyanshu.rana/spark-scripts/script.py',
                '--jdbc_url', jdbc_url,
                '--connection_properties', connection_properties,
                '--bucket_name', bucket_name_silver,
                '--prefix', ELTF
            ],
        },
    }
]

# jdbc_url, bucket_name, prefix, prefix2, connection_properties
task4_2_step = [
    {
        'Name': 'Task 2 Step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--jars', 's3://your-bucket/jars/postgresql-42.2.23.jar',
                's3://ttn-de-bootcamp-2024-bronze-us-east-1/priyanshu.rana/spark-scripts/script.py',
                '--jdbc_url', jdbc_url,
                '--connection_properties', connection_properties,
                '--bucket_name', bucket_name_silver,
                '--prefix', ELCD,
                '--prefix2', ELD
            ],
        },
    }
]

# jdbc_url, connection_properties, bucket_name, prefix, prefix2, prefix3
task4_3_step = [
    {
        'Name': 'Task 2 Step',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--jars', 's3://your-bucket/jars/postgresql-42.2.23.jar',
                's3://ttn-de-bootcamp-2024-bronze-us-east-1/priyanshu.rana/spark-scripts/script.py',
                '--jdbc_url', jdbc_url,
                '--connection_properties', connection_properties,
                '--bucket_name', bucket_name_silver,
                '--prefix', ELQD,
                '--prefix2', ELCD,
                '--prefix3', ELD
            ],
        },
    }
]
