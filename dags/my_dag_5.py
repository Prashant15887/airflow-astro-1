from airflow.decorators import task, dag
from datetime import datetime, timedelta
from groups.process_tasks import process_tasks
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
import time

partners = {
    "partner_snowflake": {
        "name": "snowflake",
        "path": "/partners/snowflake"
    },
    "partner_netflix": {
        "name": "netflix",
        "path": "/partners/netflix"
    },
    "partner_astronomer": {
        "name": "astronomer",
        "path": "/partners/astronomer"
    }
}

default_args = {
    "start_date": datetime(2025, 1, 1)
}

def _choosing_partner_based_on_day(execution_date):
    day = execution_date.day_of_week
    print(f"Execution date: {execution_date}")
    print(f"Day of week: {day}")
    if (day == 1):
        return 'extract_partner_snowflake'
    elif (day == 3):
        return 'extract_partner_netflix'
    elif (day == 5):
        return 'extract_partner_astronomer'
    else:
        return 'stop'

@dag(description="DAG in charge of processing customer data",
    default_args=default_args, schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=10), tags=["data_science","customers"],
    catchup=False, max_active_runs=1)
def my_dag_5():

    start = EmptyOperator(task_id="start")

    #choosing_partner_based_on_day = BranchPythonOperator(
    #    task_id="choosing_partner_based_on_day",
    #    python_callable=_choosing_partner_based_on_day
    #)

    #stop = EmptyOperator(task_id="stop")

    storing = EmptyOperator(task_id="storing", trigger_rule="none_failed_or_skipped")

    #choosing_partner_based_on_day >> stop

    for partner, details in partners.items():
        @task.python(task_id=f"extract_{partner}", pool='partner_pool', multiple_outputs=True)
        def extract(partner_name, partner_path):
            time.sleep(3)
            return {"partner_name": partner_name, "partner_path": partner_path}
        extracted_values = extract(details['name'], details['path'])
        start >> extracted_values
        process_tasks(extracted_values) >> storing

my_dag_5()