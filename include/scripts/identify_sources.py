import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, split, expr

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

def extract_source_info(df):
    """Genera un DataFrame con información de fuentes de noticias."""
    
    # Extraer el dominio base de la URL
    df = df.withColumn("base_url", split(col("url"), "/").getItem(2))
    
    # Crear source_name basado en news_site
    sources_df = df.groupBy("news_site").agg(
        expr("first(base_url) as url"),
        count("id").alias("article_count")
    ).withColumnRenamed("news_site", "source_name")
    
    # Calcular reliability_score basado en número de artículos publicados
    sources_df = sources_df.withColumn("reliability_score", col("article_count") / df.count())

    sources_df = sources_df.select("source_name", "url", "reliability_score")

    return sources_df

def main():
    spark = SparkSession.builder \
        .appName("News Source Identification") \
        .master("local[*]") \
        .getOrCreate()

    # Argumentos del script
    parser = argparse.ArgumentParser()
    parser.add_argument("--execution_date", required=True, help="Fecha de ejecución en formato YYYY-MM-DD")
    args = parser.parse_args()
    execution_date = args.execution_date

    logger.info(f"Procesando identificación de fuentes para la fecha: {execution_date}")

    input_path = f"./include/processed/{execution_date}/documents_clean_deduplicate.parquet"
    output_sources_path = f"./include/processed/{execution_date}/sources.csv"
    output_articles_path = f"./include/processed/{execution_date}/documents_sources.csv"

    # Cargar datos limpios
    df = spark.read.parquet(input_path)
    
    # Generar información de fuentes
    sources_df = extract_source_info(df)
    
    # Guardar fuentes en CSV
    sources_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_sources_path)
    logger.info(f"✅ Fuentes guardadas en {output_sources_path}")
    
    # Guardar relación artículos-fuentes en CSV
    df = df.select(col("id"), col("news_site").alias("source_name"))
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_articles_path)
    logger.info(f"✅ Relación artículos-fuentes guardada en {output_articles_path}")
    
    spark.stop()

if __name__ == "__main__":
    main()
