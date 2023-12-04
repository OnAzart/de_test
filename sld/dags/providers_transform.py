import json
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python import PythonOperator


def provider_to_s3f(context):
    # retrieve data from provider (assume csv file)
    # better to have specific connector, able to unify this process
    # load to s3 using boto3


def transform_to_s3f(context):
    ti = context['ti']
    file_to_process = ti.xcom_pull('extract_and_load_to_s3')

    # load file
    # transforming using pandas or spark
    # load to s3

def push_to_consumerf(consumer, context):
    ti = context['ti']
    files_to_process = ti.xcom_pull(f'transform_to_s3_{consumer}')
    connection_params = BaseHook.get_connection(f'{consumer}')
    # connecting to provider using specific Airflow  hook
    url_to_push = Variable.get(f'{consumer}_endpoint')
    # push data to provider

# new dag for new provider
with DAG(dag_id='providers_dag', schedule_interval='* 23 * * *', start_date=days_ago(1), catchup=False) as dag:
    # all operator better become custom operators including their specific needs
    provider_to_s3 = PythonOperator(task_id='provider_to_s3', python_callable=provider_to_s3f)
    transform_to_s3 = PythonOperator(task_id='transform_to_s3', python_callable=transform_to_s3f)  # can be custom operator including specific needs

    consumers = ["t1", 't2', 't3']
    with TaskGroup(group_id=f'retrive_data_group_{city}') as push_to_consumers:
        for consumer in consumers:
            push_to_consumer = PythonOperator(task_id=f'push_to_consumer_{consumer}',
                                              python_callable=push_to_consumerf,
                                              op_kwargs={'consumer': f'{consumer}'})  # can be custom operator including specific needs

    provider_to_s3 >> transform_to_s3 >> push_to_consumer

# all depends on type and size of data.
# this approach will satisfy larger files and provide idempotency and itermediate storage
# used unified approach due to no strict requirements with formats
