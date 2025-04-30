import airflow.utils.dates
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1)
}

with DAG(dag_id="cleaning_dag", default_args=default_args, schedule_interval="@daily", catchup=False) as dag:

    waiting_for_task = ExternalTaskSensor(
        task_id='waiting_for_task',
        external_dag_id='my_dag_8',
        external_task_id='storing',
        #execution_delta=
        #execution_date_fn=
        failed_states=['failed', 'skipped'],
        allowed_states=['success']
    )
    
    cleaning_xcoms = SQLExecuteQueryOperator(
        task_id='cleaning_xcoms',
        sql='sql/CLEANING_XCOMS.sql',
        conn_id='postgres'
    )

    waiting_for_task >> cleaning_xcoms