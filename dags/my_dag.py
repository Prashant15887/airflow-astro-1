# Using Jinja templating to load variables and SQL queries at runtime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from datetime import datetime, timedelta

def _extract(partner_name):
    print(partner_name)

with DAG("my_dag", description="DAG in charge of processing customer data",
         start_date=datetime(2025, 1, 1), schedule_interval="@daily",
         dagrun_timeout=timedelta(minutes=10), tags=["data_science","customers"],
         catchup=False, max_active_runs=1) as dag:
    
    extract = PythonOperator(
        task_id="extract",
        python_callable=_extract,
        op_args=["{{ var.json.my_dag_partner_1.name }}"]
    )

    fetching_data = SQLExecuteQueryOperator(
        task_id="fetching_data",
        sql="sql/MY_REQUEST.sql",
        parameters={
            'next_ds': '{{ next_ds }}',
            'prev_ds': '{{ prev_ds }}',
            'partner_name': '{{ var.json.my_dag_partner_1.name }}'
        }
    )