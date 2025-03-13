import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, split, expr

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

def extract_source_info(df):
    """
    Genera un DataFrame con información de fuentes de noticias.
    """
    logger.info("Extrayendo información de fuentes de noticias.")
    
    # Obtener el dominio base de la URL
    df = df.withColumn("base_url", split(col("url"), "/").getItem(2))
    
    # Crear source_name a partir de news_site y agregar el conteo de artículos
    sources_df = df.groupBy("news_site").agg(
        expr("first(base_url) as url"),
        count("id").alias("article_count")
    ).withColumnRenamed("news_site", "source_name")
    
    # Calcular reliability_score basado en el número de artículos publicados
    total_articles = df.count()
    if total_articles > 0:
        sources_df = sources_df.withColumn("reliability_score", col("article_count") / total_articles)
    else:
        sources_df = sources_df.withColumn("reliability_score", expr("0.0"))
    
    return sources_df.select("source_name", "url", "reliability_score")

def main():
    """
    Función principal para la identificación de fuentes de noticias.
    """
    spark = SparkSession.builder \
        .appName("News Source Identification") \
        .master("local[*]") \
        .getOrCreate()
    
    # Configurar argumentos
    parser = argparse.ArgumentParser()
    parser.add_argument("--execution_date", required=True, help="Fecha de ejecución en formato YYYY-MM-DD")
    args = parser.parse_args()
    execution_date = args.execution_date
    
    logger.info(f"Iniciando procesamiento de fuentes de noticias para la fecha: {execution_date}")
    
    input_path = f"./include/processed/{execution_date}/documents_clean_deduplicate.parquet"
    output_sources_path = f"./include/processed/{execution_date}/sources.csv"
    output_articles_path = f"./include/processed/{execution_date}/documents_sources.csv"
    
    # Cargar datos procesados
    df = spark.read.parquet(input_path)
    
    # Generar y guardar información de fuentes
    sources_df = extract_source_info(df)
    sources_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_sources_path)
    logger.info(f"Archivo de fuentes guardado en: {output_sources_path}")
    
    # Guardar relación artículos-fuentes
    articles_sources_df = df.select(col("id"), col("news_site").alias("source_name"))
    articles_sources_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_articles_path)
    logger.info(f"Archivo de relación artículos-fuentes guardado en: {output_articles_path}")
    
    spark.stop()
    logger.info("Proceso finalizado correctamente.")

if __name__ == "__main__":
    main()