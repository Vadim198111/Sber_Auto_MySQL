import datetime as dt
import os
import sys
from airflow.models import DAG
from airflow.operators.python import PythonOperator

from modules.create_tables_to_sql import creat_tables
from modules.ga_hits_to_sql import pipeline_ga_hits_new2
from modules.ga_sessions_to_sql import pipeline_ga_sessions_new2
from modules.pipeline_ga_hits_json_to_sql import pipeline
from modules.pipeline_ga_sessions_json_to_sql import pipeline2

path = os.path.expanduser('~/PycharmProjects/SberAuto')
# Добавим путь к коду проекта в переменную окружения, чтобы он был доступен python-процессу
os.environ['PROJECT_PATH'] = path
# Добавим путь к коду проекта в $PATH, чтобы импортировать функции
sys.path.insert(0, path)

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2023, 12, 14),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
}

with DAG(
        dag_id='Creat tables and insert csv-files ga_hits and ga_sessions',
        schedule_interval="@once", # Запускаем ДАГ один раз
        default_args=args,
) as dag:
    pipeline_creat_tables = PythonOperator(
        task_id='creat_tables',
        python_callable=creat_tables,
        dag=dag,
    )
    pipeline_isert_ga_hits = PythonOperator(
        task_id='pipeline_ga_hits_new',
        python_callable=pipeline_ga_hits_new2,
        dag=dag,
    )
    pipeline_insert_ga_sessions = PythonOperator(
        task_id='pipeline_ga_sessions_new',
        python_callable=pipeline_ga_sessions_new2,
        dag=dag,
    )

    pipeline_creat_tables >> pipeline_isert_ga_hits >> pipeline_insert_ga_sessions


with DAG(
        dag_id='Insert new json-files to tables',
        schedule_interval="00 15 * * *", # запускаем ДАГ 15 числа каждого месяца
        default_args=args,
) as dag:
    pipeline_insert_ga_hits_json = PythonOperator(
        task_id='pipeline_ga_hits',
        python_callable=pipeline,
        dag=dag,
    )
    pipeline_insert_ga_sessions_json = PythonOperator(
        task_id='pipeline_ga_session',
        python_callable=pipeline2,
        dag=dag,
    )
    pipeline_insert_ga_hits_json >> pipeline_insert_ga_sessions_json