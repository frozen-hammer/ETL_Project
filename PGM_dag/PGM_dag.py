from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator

#
# def create_emr_cluster():
#     cluster_id = client.run_job_flow(
#         Name="Employe_workflow",
#         Instances={
#             'InstanceGroups': [
#                 {
#                     'Name': "Master",
#                     'Market': 'ON_DEMAND',
#                     'InstanceRole': 'MASTER',
#                     'InstanceType': 'm5.xlarge',
#                     'InstanceCount': 1,
#                 },
#                 {
#                     'Name': "Slave",
#                     'Market': 'ON_DEMAND',
#                     'InstanceRole': 'CORE',
#                     'InstanceType': 'm5.xlarge',
#                     'InstanceCount': 2,
#                 }
#             ],
#             'Ec2KeyName': 'project-key-pair',
#             'KeepJobFlowAliveWhenNoSteps': True,
#             'TerminationProtected': False,
#             'Ec2SubnetId': 'subnet-049daebd20a7f7d37',
#         },
#         LogUri="s3://vishal-emr-log/",
#         ReleaseLabel='emr-6.2.1',
#         BootstrapActions=bootstrap_actions,
#         VisibleToAllUsers=True,
#         JobFlowRole="EMR_EC2_DefaultRole",
#         ServiceRole="EMR_DefaultRole",
#         Applications=[{'Name': 'Spark'}, {'Name': 'Hive'}]
#     )
#
#     print(f"The cluster started with cluster id : {cluster_id}")
#     return cluster_id
#
#
#
#
#
# def add_step_emr(cluster_id, step_name, jar_file, step_args):
#     print("The cluster id : {}".format(cluster_id))
#     print("The step to be added : {}".format(step_args))
#     response = client.add_job_flow_steps(
#         JobFlowId=cluster_id,
#         Steps=[
#             {
#                 'Name': step_name,
#                 'ActionOnFailure': 'CONTINUE',
#                 'HadoopJarStep': {
#                     'Jar': jar_file,
#                     'Args': step_args
#                 }
#             }
#         ]
#     )
#     print("The emr step is added")
#     return response['StepIds'][0]
#
#
#
#
#
# def terminate_cluster(cluster_id):submit_spark_job = EmrAddStepsOperator(
#     task_id='submit_spark_job',
#     job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
#     steps=[
#         {
#             'Name': 'Spark Job',
#             'ActionOnFailure': 'CONTINUE',
#             'HadoopJarStep': {
#                 'Jar': 'command-runner.jar',
#                 'Args': ['spark-submit',
#                          '--deploy-mode',
#                          'cluster',
#                          '--jars',
#                          's3://ttn-de-bootcamp-2024-bronze-us-east-1/priyanshu.rana/jars/postgresql-42.6.2.jar',
#                          's3://ttn-de-bootcamp-2024-bronze-us-east-1/priyanshu.rana/spark-scripts/script.py'],
#             },
#         },
#     ],
# #     try:
#         client.terminate_job_flows(JobFlowIds=[cluster_id])
#         logger.info("Terminated cluster %s.", cluster_id)
#     except ClientError:
#         logger.exception("Couldn't terminate cluster %s.", cluster_id)
#         raise
#
#
#
#


from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator
# from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
# from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
# from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
# from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
# from airflow.providers.amazon.aws.operators.emr import EmrStepSensor

from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'emr_postgres_dag',
    default_args=default_args,
    description='A DAG to create EMR cluster and post data to Postgres',
    schedule_interval='@once',
)



# Read path of file from S3 using boto3
# pass it to the pyspark job using spark submit

submit_spark_job = EmrAddStepsOperator(
    task_id='submit_spark_job',
    job_flow_id="j-3IRPCMQIR4KIR",
    steps=[
        {
            'Name': 'Spark Job',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit',
                         '--deploy-mode',
                         'cluster',
                         '--jars',
                         's3://ttn-de-bootcamp-2024-bronze-us-east-1/priyanshu.rana/jars/postgresql-42.6.2.jar',
                         's3://ttn-de-bootcamp-2024-bronze-us-east-1/priyanshu.rana/spark-scripts/script.py'],
            },
        },
    ],
    aws_conn_id='aws_default',
    dag=dag,
)

wait_for_spark_job_completion = EmrStepSensor(
    task_id='wait_for_spark_job_completion',
    job_flow_id="j-3IRPCMQIR4KIR",
    step_id="{{ task_instance.xcom_pull(task_ids='submit_spark_job', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag,
)



submit_spark_job >> wait_for_spark_job_completion
