from airflow.decorators import task, dag
from datetime import datetime, timedelta
from groups.process_tasks import process_tasks
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
import time
from airflow.sensors.date_time import DateTimeSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

partners = {
    "partner_snowflake": {
        "name": "snowflake",
        "path": "/partners/snowflake",
        "priority": 2
    },
    "partner_netflix": {
        "name": "netflix",
        "path": "/partners/netflix",
        "priority": 3
    },
    "partner_astronomer": {
        "name": "astronomer",
        "path": "/partners/astronomer",
        "priority": 1
    }
}

default_args = {
    "start_date": datetime(2025, 1, 1),
    "retries": 0
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

def _success_callback(context):
    print(context)

def _failure_callback(context):
    print(context)

def _extract_callback_success(context):
    print('SUCCESS CALLBACK')

def _sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    print(task_list)
    print(blocking_tis)
    print(slas)

#from airflow.exceptions import AirflowTaskTimeout, AirflowSensorTimeout
def _extract_callback_failure(context):
    #if (context['exception']):
    #    if (isinstance(context['exception'], AirflowTaskTimeout)):
    #    if (isinstance(context['exception'], AirflowSensorTimeout)):
    print('FAILURE CALLBACK')

def _extract_callback_retry(context):
    #if (context['ti'].try_number() > 2):
    print('RETRY CALLBACK')

@dag(description="DAG in charge of processing customer data",
    default_args=default_args, schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=10), tags=["data_science","customers"],
    catchup=False, max_active_runs=1, on_success_callback=_success_callback, on_failure_callback=_failure_callback,
    sla_miss_callback=_sla_miss_callback)
def my_dag_8():

    start = EmptyOperator(task_id="start", trigger_rule='dummy', pool='default_pool', execution_timeout=timedelta(minutes=10))

    delay = DateTimeSensor(
        task_id="delay",
        target_time="{{ execution_date.add(hours=9) }}",
        poke_interval=60 * 60,
        mode="reschedule",
        timeout=60 * 60 * 10,
        #execution_timeout=
        soft_fail=True,
        exponential_backoff=True,
    )

    #choosing_partner_based_on_day = BranchPythonOperator(
    #    task_id="choosing_partner_based_on_day",
    #    python_callable=_choosing_partner_based_on_day
    #)

    #stop = EmptyOperator(task_id="stop")

    storing = EmptyOperator(task_id="storing", trigger_rule="none_failed_or_skipped")

    trigger_cleaning_xcoms = TriggerDagRunOperator(
        task_id='trigger_cleaning_xcoms',
        trigger_dag_id='cleaning_dag',
        execution_date='{{ ds }}',
        wait_for_completion=True,
        poke_interval=60,
        reset_dag_run=True,
        failed_states=['failed']
    )

    #choosing_partner_based_on_day >> stop

    for partner, details in partners.items():
        @task.python(task_id=f"extract_{partner}", depends_on_past=True, priority_weight=details['priority'], pool='partner_pool', multiple_outputs=True,
                     sla=timedelta(minutes=5),
                     retries=3, retry_delay=timedelta(minutes=5), retry_exponential_backoff=True, max_retry_delay=timedelta(minutes=15),
                     on_success_callback=_extract_callback_success, on_failure_callback=_extract_callback_failure, on_retry_callback=_extract_callback_retry)
        def extract(partner_name, partner_path):
            time.sleep(3)
            #raise ValueError("failed")
            return {"partner_name": partner_name, "partner_path": partner_path}
        extracted_values = extract(details['name'], details['path'])
        start >> extracted_values
        process_tasks(extracted_values) >> storing

my_dag_8()