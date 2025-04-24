from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task, dag
# from airflow.operators.subdag import SubDagOperator
# from airflow.utils.task_group import TaskGroup

from datetime import datetime, timedelta
# from subdag.subdag_factory import subdag_factory
from groups.process_tasks import process_tasks

@task.python(task_id="extract_partners", multiple_outputs=True)
def extract():
    partner_name = "netflix"
    partner_path = "/partners/netflix"
    return {"partner_name": partner_name, "partner_path": partner_path}

default_args = {
    "start_date": datetime(2025, 1, 1)
}

@dag(description="DAG in charge of processing customer data",
    default_args=default_args, schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=10), tags=["data_science","customers"],
    catchup=False, max_active_runs=1)
def my_dag_2():

    partner_settings = extract()

    process_tasks(partner_settings)

    # process_tasks = SubDagOperator(
    #    task_id="process_tasks",
    #    subdag=subdag_factory("my_dag_2", "process_tasks", default_args)
    #)

    # extract() >> process_tasks

my_dag_2()