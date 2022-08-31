# pip install apache-airflow-providers-postgres[amazon]

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

DAG_ID = "postgres_operator_dag"

args = {
'owner':'airflow',
'start_date':datetime(2022,8,30),
'depends_on_past': False,
'schedule_interval':'*/5 * * * *',
'dagrun_timeout':timedelta(seconds=5)
}

with DAG(dag_id=DAG_ID, description = 'example dags postgres', catchup=False, default_args=args,tags=['postgres']) as dag:
	record_generation = PostgresOperator(task_id='record_generation', postgres_conn_id='postgres_test', sql='sql/query1.sql')
	final_report = PostgresOperator(task_id='final_report', postgres_conn_id='postgres_test', sql='sql/query2.sql')

record_generation >> final_report