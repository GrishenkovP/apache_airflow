import pandas as pd
import random
import airflow
from airflow.models import Variable
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
import datetime
from faker import Faker


args = {
'owner':'airflow',
'start_date':airflow.utils.dates.days_ago(1),
'schedule_interval':None,
}


path_data = Variable.get('path_data')
path_data_full = "/".join([path_data,'data','result.csv'])
cmd_command = 'mkdir -p ' + path_data + '/data'

rows_number = int(Variable.get('rows_number'))

def generation_dataframe(rows_number=rows_number):

    fake = Faker()
    start_date = datetime.date(year=2020,month=1,day=1)
    finish_date = datetime.date(year=2020,month=12,day=31)
    region_list_name = ['r1','r2','r3']
    manager_list_name = ['m1','m2', 'm3','m4', 'm5']
    frames = []

    for r in region_list_name:
        date_list = []
        region_list = []
        manager_list = []
        amount_list = []
        for _ in range(rows_number):
            date_list.append(fake.date_between(start_date= start_date, end_date = finish_date))
            region_list.append(r)
            manager_list.append(manager_list_name[random.randint(0,4)])
            amount_list.append(random.randint(0,100))
        d = {'dt':date_list,'region':region_list,'manager':manager_list,'amount':amount_list}
        df = pd.DataFrame(data=d)
        frames.append(df)

    df_result = pd.concat(frames)
    df_result.to_csv(path_data_full,sep=',',index=False)

def generation_report():
    df = pd.read_csv(path_data_full,sep=',')
    df['dt'] = pd.to_datetime(df['dt'], format='%Y-%m-%d')
    df_report = df.groupby(['region','manager'],as_index=False)['amount'].sum()
    mean_total_amount = round(df_report['amount'].mean(),1)
    return mean_total_amount
    

with DAG(dag_id = 'example_dags', description = 'example dags python', catchup=False, default_args=args,tags=['example']) as dag:
    create_folder = BashOperator(task_id = 'create_folder', bash_command=cmd_command)
    create_dataset = PythonOperator(task_id = 'create_dataset', python_callable = generation_dataframe)
    wait_file = FileSensor(task_id='wait_file_result', filepath=path_data_full)
    calculate_sales = PythonOperator(task_id = 'calculate_sales', python_callable = generation_report)

create_folder >> create_dataset >> wait_file >>calculate_sales
    

