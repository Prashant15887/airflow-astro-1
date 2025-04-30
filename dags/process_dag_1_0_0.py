from airflow.models import DAG
from airflow.decorators import task

from datetime import datetime

default_args = {
    'start_date': datetime(2025, 1, 1)
}

with DAG('process_dag_1_0_0', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:

    @task.python
    def t1():
        print("t1")

    @task.python
    def t2():
        print("t2")

    t1() >> t2()