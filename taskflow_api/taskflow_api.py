import os
from datetime import datetime, timedelta
import pandas as pd
from airflow.decorators import dag, task
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
FILE_NAME_FINISH = 'data3.csv'


@dag(dag_id='etl_sale_taskflow_api', description='etl_sale_dataset', catchup=False, default_args=args, tags=['example']) 
def taskflow_api_etl():

	@task()
	def extract_data():
	    date_parser = lambda x: datetime.strptime(x, '%d.%m.%Y')
	    df_csv = pd.read_csv(os.path.join(PATH_FILE,FILE_NAME_START), 
		             sep=';',
		             parse_dates=['date'],
		             date_parser=date_parser)
	    print(df_csv)   
	    return df_csv

	@task
	def transform_data(df_csv):
	    df_sale = pd.DataFrame(df_csv)
	    df_sale['percent']= df_sale['amount'].apply(lambda x: round(x/100*25,2))
	    print(df_sale)
	    return df_sale

	@task
	def load_data(df_sale):
	    df_sale_finish = pd.DataFrame(df_sale)
	    print(df_sale_finish)
	    df_sale_finish.to_csv(os.path.join(PATH_FILE,FILE_NAME_FINISH), index=False)


	extract_data = extract_data()
	transform_data = transform_data(extract_data)
	load_data = load_data(transform_data)

etl_dag = taskflow_api_etl()

