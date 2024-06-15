# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from airflow.utils.dates import days_ago
# from datetime import date

# from lib.combine_data import main

# main()


# call_ANI_data_fetcher >> call_raw_to_fmt_ANI >> call_combine_data >> call_elastic_insert
# call_MAL_data_fetcher >> call_raw_to_fmt_MAL >> call_combine_data >> call_elastic_insert
