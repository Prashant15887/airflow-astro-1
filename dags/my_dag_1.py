from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task, dag

from datetime import datetime, timedelta

@task.python(task_id="extract_partners", multiple_outputs=True)
def extract():
    partner_name = "netflix"
    partner_path = "/partners/netflix"
    # ti.xcom_push(key="partner_name", value=partner_name)
    return {"partner_name": partner_name, "partner_path": partner_path}

@task.python
def process(partner_name, partner_path):
    # partner_name = ti.xcom_pull(key="partner_name", task_ids="extract")
    # partner_name = ti.xcom_pull(key="return_value", task_ids="extract")
    # partner_settings = ti.xcom_pull(task_ids="extract")
    # print(partner_settings['partner_name'])
    print(partner_name)
    print(partner_path)

@dag(description="DAG in charge of processing customer data",
         start_date=datetime(2025, 1, 1), schedule_interval="@daily",
         dagrun_timeout=timedelta(minutes=10), tags=["data_science","customers"],
         catchup=False, max_active_runs=1)
def my_dag_1():

    partner_settings = extract()
    process(partner_settings['partner_name'], partner_settings['partner_path'])
    #process(extract())

my_dag_1()