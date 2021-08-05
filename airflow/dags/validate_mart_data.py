from datetime import datetime

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

VALIDATION_TIMEFRAME = 10 * 60

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'email': ['twdu7-india@thoughtworks.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0
}

dag = DAG('validate_mart_data',
          catchup=False,
          default_args=default_args,
          schedule_interval="*/5 * * * *")


def file_watcher_task():
    hadoop_host = Variable.get(key="hadoop_host")
    hadoop_port = Variable.get(key="hadoop_port", default_var="50070")
    url = "{0}:{1}/webhdfs/v1/tw/stationMart/data/_SUCCESS?op=LISTSTATUS".format(hadoop_host, hadoop_port)
    response = requests.get(url)
    response.raise_for_status()
    response_json = response.json()
    success_file = response_json['FileStatuses']['FileStatus'][0]
    latest_modification_time = success_file['modificationTime']
    if (datetime.now() - datetime.fromtimestamp(latest_modification_time / 1000)).seconds > VALIDATION_TIMEFRAME:
        raise Exception("No new file has been processed in last 10 mins!!")


t1 = PythonOperator(
    task_id='monitor_delivery_file',
    python_callable=file_watcher_task,
    dag=dag)
