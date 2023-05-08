import datetime as dt
import os
import sys
import logging

from airflow.models import DAG
from airflow.operators.python import PythonOperator


path = os.path.expanduser(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                       'airflow_hw'))
# Добавим путь к коду проекта в переменную окружения, чтобы он был доступен python-процессу
os.environ['PROJECT_PATH'] = path
# Добавим путь к коду проекта в $PATH, чтобы импортировать функции
sys.path.insert(0, path)

from modules.pipeline import pipeline
from modules.predict import predict


def _pipeline(ti):
    model_file_name = pipeline()
    logging.info(f'the best model file name is {model_file_name}')
    ti.xcom_push(key='model_file_name', value=model_file_name)


def _predict(ti):
    model_file_name = ti.xcom_pull(key='model_file_name', task_ids='pipeline')
    logging.info(f'the best model file name is {model_file_name}')
    predict(model_file_name)


args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2023, 4, 25),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
}

with DAG(
        dag_id='car_price_prediction',
        schedule="@once",
        default_args=args,
) as dag:
    task_pipeline = PythonOperator(
        task_id='pipeline',
        python_callable=_pipeline,
    )

    task_predict = PythonOperator(
        task_id='predict',
        python_callable=_predict,
    )
    # <YOUR_CODE>
    task_pipeline >> task_predict
