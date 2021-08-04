import requests
from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta

from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

VALIDATION_TIMEFRAME = 10 * 60
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(0, hour=1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

dag = DAG('delivery_file_monitoring',
          default_args=default_args,
          schedule_interval="*/5 * * * *")


def file_watcher_command():
    training_cohort = Variable.get(key="training_cohort", default_var="twdu7-in-oz")
    url = "http://emr-master.%s.training:50070/webhdfs/v1/tw/stationMart/data/_SUCCESS?op=LISTSTATUS" % training_cohort
    response = requests.get(url)
    response.raise_for_status()
    response_json = response.json()
    success_file = response_json['FileStatuses']['FileStatus'][0]
    latest_modification_time = success_file['modificationTime']
    if (datetime.now() - datetime.fromtimestamp(latest_modification_time / 1000)).seconds > VALIDATION_TIMEFRAME:
        raise Exception("No new file has been processed in last 10 mins!!")


t1 = PythonOperator(
    task_id='monitor_delivery_file',
    python_callable=file_watcher_command,
    dag=dag)
