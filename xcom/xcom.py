import os
from datetime import datetime, timedelta
import pandas as pd
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

args = {
    'owner':'airflow',
    'depends_on_past':False,
    'start_date':datetime(2021,5,14),
    'retries':1,
    'retry_delay':timedelta(minutes=1),
    'schedule_interval':'@once',    
        }

PATH_FILE = '/dataset/'
FILE_NAME_START = 'data.csv'
FILE_NAME_FINISH = 'data2.csv'

def extract_data(**kwargs):
    ti = kwargs['ti']
    date_parser = lambda x: datetime.strptime(x, '%d.%m.%Y')
    df_csv = pd.read_csv(os.path.join(PATH_FILE,FILE_NAME_START), 
                     sep=';',
                     parse_dates=['date'],
                     date_parser=date_parser)
    print(df_csv)   
    ti.xcom_push(key='sale_csv', value=df_csv)


def transform_data(**kwargs):
    ti = kwargs['ti']
    df_sale = ti.xcom_pull(key='sale_csv',task_ids=['extract_data'])[0]
    df_sale = pd.DataFrame(df_sale)
    df_sale['percent']= df_sale['amount'].apply(lambda x: round(x/100*25,2))
    print(df_sale)
    ti.xcom_push(key='sale_df', value=df_sale)

def load_data(**kwargs):
    ti = kwargs['ti']
    df_sale_finish = ti.xcom_pull(key='sale_df',task_ids=['transform_data'])[0]  
    df_sale_finish = pd.DataFrame(df_sale_finish)
    print(df_sale_finish)
    df_sale_finish.to_csv(os.path.join(PATH_FILE,FILE_NAME_FINISH), index=False)


with DAG(dag_id='etl_sale_xcom', description='etl_sale_dataset', catchup=False, default_args=args, tags=['example']) as dag:

    extract_data    = PythonOperator(task_id='extract_data', python_callable=extract_data)
    transform_data  = PythonOperator(task_id='transform_data', python_callable=transform_data)
    load_data       = PythonOperator(task_id='load_data', python_callable=load_data)

    extract_data >> transform_data >> load_data

