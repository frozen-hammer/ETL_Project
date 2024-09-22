from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor


from PGM_dag.sql.create_tbl import create_tables_sql
from PGM_dag.sql.sql_task_2 import compare_staging_employee_ts_table_query
from PGM_dag.sql.task_3_2 import employee_leave_data_transformation_sql

from PGM_dag.python_file.S3_silver_push import Save_to_S3

# from PGM_dag.spark_script.task1 import read_data
# from PGM_dag.spark_script.task2 import transform_timestamp_data
# from PGM_dag.spark_script.task4_1 import task_4_1_transformation
# from PGM_dag.spark_script.task4_2 import task_4_2_transformation
# from PGM_dag.spark_script.task4_3 import task_4_3_transformation

from PGM_dag.steps.steps import task1_step
from PGM_dag.steps.steps import task2_step
from PGM_dag.steps.steps import task4_1_step
from PGM_dag.steps.steps import task4_2_step
from PGM_dag.steps.steps import task4_3_step


default_args = {
    'owner': 'madhav',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# Define the DAG
with DAG('GPM_Dag',
         default_args=default_args,
         schedule_interval=None) as dag:
    start = DummyOperator(
        task_id='start',
        dag=dag,
    )

    create_tables = PostgresOperator(
        task_id='create_tables',
        sql=create_tables_sql,
        postgres_conn_id='postgres_conn',  # Connection ID configured in Airflow UI
        autocommit=True,  # Ensure autocommit is set to True
        dag=dag
    )

    # Add the step to the EMR cluster
    task1_add_step = EmrAddStepsOperator(
        task_id='add_step',
        job_flow_id=CLUSTER_ID,
        steps=task1_step,
        aws_conn_id='aws_default',
        dag=dag,
    )

    # Wait for the step to complete
    task1_step_checker = EmrStepSensor(
        task_id='watch_step_1',
        job_flow_id=CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='add_step', key='return_value')[0] }}",
        aws_conn_id='aws_default',
        dag=dag,
    )

    # Add the step to the EMR cluster
    task2_add_step = EmrAddStepsOperator(
        task_id='add_step_2',
        job_flow_id=CLUSTER_ID,
        steps=task2_step,
        aws_conn_id='aws_default',
        dag=dag,
    )

    # Wait for the step to complete
    task2_step_checker = EmrStepSensor(
        task_id='watch_step_2',
        job_flow_id=CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='add_step', key='return_value')[0] }}",
        aws_conn_id='aws_default',
        dag=dag,
    )

    compare_ts_stage_final_data = PostgresOperator(
        task_id="compare_ts_stage_final_data",
        postgres_conn_id='postgres_conn',
        sql=compare_staging_employee_ts_table_query
    )

    # TASK 3_1 HERE......

    # Task 3_2
    daily_leave_tracking_task_3_2 = PostgresOperator(
        task_id="daily_leave_tracking_task_3_2",
        postgres_conn_id='postgres_conn',
        sql=employee_leave_data_transformation_sql
    )

    # Add the step to the EMR cluster
    task4_1_add_step = EmrAddStepsOperator(
        task_id='add_step_4_1',
        job_flow_id=CLUSTER_ID,
        steps=task4_1_step,
        aws_conn_id='aws_default',
        dag=dag,
    )

    # Wait for the step to complete
    task4_1_step_checker = EmrStepSensor(
        task_id='watch_step_4_1',
        job_flow_id=CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='add_step', key='return_value')[0] }}",
        aws_conn_id='aws_default',
        dag=dag,
    )

    # Add the step to the EMR cluster
    task4_2_add_step = EmrAddStepsOperator(
        task_id='add_step_4_2',
        job_flow_id=CLUSTER_ID,
        steps=task4_1_step,
        aws_conn_id='aws_default',
        dag=dag,
    )

    # Wait for the step to complete
    task4_2_step_checker = EmrStepSensor(
        task_id='watch_step_4_2',
        job_flow_id=CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='add_step', key='return_value')[0] }}",
        aws_conn_id='aws_default',
        dag=dag,
    )

    # Add the step to the EMR cluster
    task4_3_add_step = EmrAddStepsOperator(
        task_id='add_step_4_1',
        job_flow_id=CLUSTER_ID,
        steps=task4_3_step,
        aws_conn_id='aws_default',
        dag=dag,
    )

    # Wait for the step to complete
    task4_3_step_checker = EmrStepSensor(
        task_id='watch_step_4_3',
        job_flow_id=CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='add_step', key='return_value')[0] }}",
        aws_conn_id='aws_default',
        dag=dag,
    )

    end = DummyOperator(
        task_id='end',
        dag=dag,
    )

start >> create_tables >> task1_add_step >> task1_step_checker >> task2_add_step >> task2_step_checker >> compare_ts_stage_final_data >> daily_leave_tracking_task_3_2 >> task4_1_add_step >> task4_1_step_checker >> task4_2_add_step >> task4_2_step_checker >> task4_3_add_step >> task4_3_step_checker >> end