import os
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("PySpark Example") \
        .master("local[*]") \
        .getOrCreate()
        
    clean_deduplicate_path = "./include/processed/2024-01-01/documents_clean_deduplicate.parquet"
    sentiment_analysis_path = "./include/processed/2024-01-01/sentiment_analysis.parquet"
    identify_topics_path = "./include/processed/2024-01-01/documents_with_topics.parquet"
    topics = "./include/processed/2024-01-01/topics.parquet"
        
    df = spark.read.parquet(clean_deduplicate_path)
    df.show()  # 
    
    df = spark.read.parquet(sentiment_analysis_path)
    df.show()  # 

    df = spark.read.parquet(identify_topics_path)
    df.show()  # 

    df = spark.read.parquet(topics)
    df.show()  # 

    # os.makedirs(output_path, exist_ok=True)
    # # Guardar DataFrame en formato Parquet
    # df.write.mode("overwrite").parquet(output_path)
    
    spark.stop()    

if __name__ == "__main__":
    main()
