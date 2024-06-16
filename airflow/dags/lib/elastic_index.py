import json
from elasticsearch import Elasticsearch, helpers
import pandas as pd
import pyarrow.parquet as pq
import os
from datetime import date

# Ensure the URL includes the port, usually 443 for HTTPS if not specified.
es = Elasticsearch(
    ["https://my-first-deployment-3ab24b.es.us-central1.gcp.cloud.es.io:443"],
    basic_auth=("elastic", "NzZISODVwO04jqPNjWUQPcCZ")
)

def load_parquet_to_es(file_path, index_name, id_field):
    try:
        print(f"Checking file: {file_path}")
        print(f"File exists: {os.path.exists(file_path)}")
        print(f"File readable: {os.access(file_path, os.R_OK)}")

        table = pq.read_table(file_path)
        df = table.to_pandas()

        df = df.where(pd.notnull(df), None)

        actions = [
            {
                "_index": index_name,
                "_id": record[id_field],  
                "_source": record
            }
            for record in df.to_dict(orient='records')
        ]

        success, failed = helpers.bulk(es, actions, stats_only=False, raise_on_error=False)
        
        print(f"Documents successfully indexed: {success}")
        print(f"Documents failed to index: {len(failed)}")
        
        if failed:
            with open("failed_documents.log", "w") as f:
                for failure in failed:
                    f.write(json.dumps(failure, indent=2))
            print("Failed documents logged to failed_documents.log")
            
    except PermissionError as e:
        print(f"Permission error: {e}")
    except FileNotFoundError as e:
        print(f"File not found: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

def main():
    current_day = date.today().strftime("%Y%m%d")
    load_parquet_to_es(f"datalake/usage/animeAnalysis/AnimeTop300/{current_day}/combined_ratings.snappy.parquet", "anime_stats", "id")  
