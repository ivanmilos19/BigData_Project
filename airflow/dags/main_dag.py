from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import date, datetime, timedelta

from lib.data_fetcher.fetch_ANI_anime_data import main as fetch_ANI_data_fetcher
from lib.data_fetcher.fetch_MAL_anime_data import main as fetch_MAL_data_fetcher
from lib.raw_to_fmt.raw_to_fmt_ANI import main as raw_to_fmt_ANI
from lib.raw_to_fmt.raw_to_fmt_MAL import main as raw_to_fmt_MAL
from lib.combine_data import main as combine_data
from lib.elastic_index import main as elastic_index


with DAG(
       'main_dag',
       default_args={
           'depends_on_past': False,
           'email': ['airflow@example.com'],
           'email_on_failure': False,
           'email_on_retry': False,
           'retries': 1,
           'retry_delay': timedelta(minutes=5),
       },
       description='BigData Project Main DAG',
       schedule_interval=None,
       start_date=datetime(2021, 1, 1),
       catchup=False,
) as dag:
   dag.doc_md = """
       Main dag that runs the project, extracting, transforming, combining and indexing.
   """

call_ANI_data_fetcher = PythonOperator(
       task_id='source_tor_raw_ANI',
       python_callable=fetch_ANI_data_fetcher,
       dag=dag,
)

call_MAL_data_fetcher = PythonOperator(
       task_id='source_tor_raw_MAL',
       python_callable=fetch_MAL_data_fetcher,
       dag=dag,
)

call_raw_to_fmt_ANI = PythonOperator(
       task_id='raw_to_formatted_ANI',
       python_callable=raw_to_fmt_ANI,
       dag=dag,
)

call_raw_to_fmt_MAL = PythonOperator(
       task_id='raw_to_formatted_MAL',
       python_callable=raw_to_fmt_MAL,
       dag=dag,
)

call_combine_data = PythonOperator(
       task_id='produce_usage',
       python_callable=combine_data,
       dag=dag,
)


call_elastic_insert = PythonOperator(
       task_id='index_to_elastic',
       python_callable=elastic_index,
       dag=dag,
)


call_ANI_data_fetcher >> call_raw_to_fmt_ANI >> call_combine_data >> call_elastic_insert
call_MAL_data_fetcher >> call_raw_to_fmt_MAL >> call_combine_data >> call_elastic_insert
