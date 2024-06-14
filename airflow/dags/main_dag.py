from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime


import lib.data_fetcher.fetch_ANI_anime_data 
import lib.data_fetcher.fetch_MAL_anime_data
import lib.raw_to_fmt.raw_to_fmt_ANI
import lib.raw_to_fmt.raw_to_fmt_MAL
import lib.combine_data
import lib.elastic_index

dag = DAG(
    'main_dag',
    default_args={'start_date': days_ago(1)},
    schedule_interval='*/5 * * * *',
    catchup=False
)

call_ANI_data_fetcher = PythonOperator(
    task_id='source_to_raw_1',
    python_callable=lib.data_fetcher.fetch_ANI_anime_data ,
    dag=dag
)

call_MAL_data_fetcher = PythonOperator(
    task_id='source_to_raw_2',
    python_callable=lib.data_fetcher.fetch_MAL_anime_data,
    dag=dag
)

call_raw_to_fmt_ANI = PythonOperator(
    task_id='raw_to_formatted_1',
    python_callable=lib.raw_to_fmt.raw_to_fmt_ANI,
    dag=dag
)

call_raw_to_fmt_MAL = PythonOperator(
    task_id='raw_to_formatted_2',
    python_callable=lib.raw_to_fmt.raw_to_fmt_MAL,
    dag=dag
)

call_combine_data = PythonOperator(
    task_id='produce_usage',
    python_callable=lib.combine_data,
    dag=dag
)

call_elastic_insert = PythonOperator(
    task_id='index_to_elastic',
    python_callable=lib.elastic_index,
    dag=dag
)

call_ANI_data_fetcher >> call_raw_to_fmt_ANI >> call_combine_data >> call_elastic_insert
call_MAL_data_fetcher >> call_raw_to_fmt_MAL >> call_combine_data >> call_elastic_insert
