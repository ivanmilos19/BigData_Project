import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, array, lit, current_timestamp
from datetime import date

# DATALAKE_ROOT_FOLDER = "C:/BigData_project/"

def combine_data(current_day):
    try:
        
        RATING_PATH_MAL = os.path.join("datalake", "formatted", "MAL", "Top_anime", current_day)
        RATING_PATH_ANI = os.path.join("datalake", "formatted", "ANI", "Top_anime", current_day)
        USAGE_OUTPUT_FOLDER_BEST = os.path.join("datalake", "usage", "animeAnalysis", "AnimeTop300", current_day)

        
        if not os.path.exists(USAGE_OUTPUT_FOLDER_BEST):
            print("combine data creating folder", os.getcwd(), USAGE_OUTPUT_FOLDER_BEST)
            os.makedirs(USAGE_OUTPUT_FOLDER_BEST)

        
        spark = SparkSession.builder.appName("CombineData").getOrCreate()

        
        df_ratings_mal = spark.read.parquet(RATING_PATH_MAL)
        df_ratings_ani = spark.read.parquet(RATING_PATH_ANI)

        
        df_ratings_mal = df_ratings_mal.select(
            col("id").alias("id"), 
            col("title.english").alias("english_title_MAL"),
            col("averageScore").alias("averageScore_MAL"),
            col("ranking").alias("ranking_MAL"),
            col("genres").alias("genres_MAL")
        )
        
        df_ratings_ani = df_ratings_ani.select(
            col("id").alias("id"), 
            col("title.english").alias("english_title_ANI"),
            col("averageScore").alias("averageScore_ANI"),
            col("ranking").alias("ranking_ANI"),
            col("genres").alias("genres_ANI")
        )

        
        df_ratings = df_ratings_mal.join(df_ratings_ani, on="id", how="outer")

        
        df_ratings = df_ratings.fillna({
            'english_title_MAL': '',
            'averageScore_MAL': -1,
            'ranking_MAL': -1,
            'english_title_ANI': '',
            'averageScore_ANI': -1,
            'ranking_ANI': -1
        })

        
        df_ratings = df_ratings.withColumn(
            "genres_MAL",
            when(col("genres_MAL").isNull(), array()).otherwise(col("genres_MAL"))
        ).withColumn(
            "genres_ANI",
            when(col("genres_ANI").isNull(), array()).otherwise(col("genres_ANI"))
        )

        
        df_ratings = df_ratings.withColumn("timestamp", current_timestamp())

        
        df_ratings.show(truncate=False)
        
        print(f"Combined DataFrame has {df_ratings.count()} rows")

        print(f"Writing combined DataFrame to {USAGE_OUTPUT_FOLDER_BEST}")
        
        output_path = os.path.join(USAGE_OUTPUT_FOLDER_BEST, "combined_ratings.snappy.parquet")
        df_ratings.write.mode("overwrite").parquet(output_path)

        
        spark.stop()
    except Exception as e:
        print(f"An error occurred: {e}")
        raise
    
def main():
    current_day = date.today().strftime("%Y%m%d")
    combine_data(current_day)


