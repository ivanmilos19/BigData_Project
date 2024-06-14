import os
import pandas as pd
from datetime import date

# DATALAKE_ROOT_FOLDER = "C:/BigData_project/"

#  DATALAKE_ROOT_FOLDER, "airflow", 

def convert_raw_to_formatted(file_name, current_day):
    RATING_PATH = os.path.join("datalake", "raw", "ANI", "Top_anime", current_day, file_name)
    FORMATTED_RATING_FOLDER = os.path.join("datalake", "formatted", "ANI", "Top_anime", current_day)
    if not os.path.exists(FORMATTED_RATING_FOLDER):
        print("raw ANI creating folder", os.getcwd(), FORMATTED_RATING_FOLDER)
        os.makedirs(FORMATTED_RATING_FOLDER)
    
    df = pd.read_json(RATING_PATH)
    
    parquet_file_name = file_name.replace(".json", ".snappy.parquet")
    df.to_parquet(os.path.join(FORMATTED_RATING_FOLDER, parquet_file_name), engine='pyarrow')

def main():
    current_day = date.today().strftime("%Y%m%d")
    convert_raw_to_formatted("ANI_top_anime.json", current_day)






