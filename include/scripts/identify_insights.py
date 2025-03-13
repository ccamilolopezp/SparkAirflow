import argparse
import logging
import spacy
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, count, desc, lower, udf
from pyspark.ml.feature import StopWordsRemover
from pyspark.sql.types import ArrayType, StringType

# Configuración de logging
logger = logging.getLogger("NewsAnalysis")
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler("spaceflight_news.log", mode='w')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Cargar el modelo de procesamiento de lenguaje natural
nlp = spacy.load("en_core_web_sm")

def extract_words(df):
    """Extrae palabras clave del resumen de noticias, eliminando stopwords."""
    if df is None:
        logger.warning("El DataFrame proporcionado es inválido.")
        return None
    
    logger.info("Extrayendo palabras clave de los resúmenes de noticias.")
    df = df.withColumn("words", split(lower(col("summary")), " "))
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    df = remover.transform(df)
    df_filtered = df.select(explode(col("filtered_words")).alias("filtered_words"))
    
    return df_filtered.groupBy("filtered_words").count().orderBy("count", ascending=False)

def extract_entities(df):
    """Extrae entidades nombradas de los resúmenes de noticias."""
    if df is None:
        logger.warning("El DataFrame proporcionado es inválido.")
        return None
    
    logger.info("Extrayendo entidades nombradas de los resúmenes de noticias.")
    
    def extract_entities_from_text(text):
        doc = nlp(text)
        return [f"{ent.text} ({ent.label_})" for ent in doc.ents]
    
    extract_entities_udf = udf(extract_entities_from_text, ArrayType(StringType()))
    df = df.withColumn("entities", extract_entities_udf(col("summary")))
    
    return df.select("id", "entities")

def analyze_trends(df):
    """Analiza la cantidad de artículos publicados a lo largo del tiempo."""
    if df is None:
        logger.warning("El DataFrame proporcionado es inválido.")
        return None
    
    logger.info("Analizando tendencias de publicación de noticias.")
    return df.groupBy("published_at").count().orderBy(desc("published_at"))

def analyze_news_sources(df):
    """Analiza la cantidad de artículos publicados por cada fuente de noticias."""
    if df is None:
        logger.warning("El DataFrame proporcionado es inválido.")
        return None
    
    logger.info("Analizando frecuencia de aparición de fuentes de noticias.")
    return df.groupBy("news_site").count().orderBy(desc("count"))

def main():
    """Función principal para la ejecución del análisis."""
    spark = SparkSession.builder \
        .appName("News Data Analysis") \
        .master("local[*]") \
        .getOrCreate()
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--execution_date", required=True, help="Fecha de ejecución en formato YYYY-MM-DD")
    args = parser.parse_args()
    
    execution_date = args.execution_date
    logger.info(f"Iniciando procesamiento de datos para la fecha: {execution_date}")

    input_path = f"./include/processed/{execution_date}/documents_clean_deduplicate.parquet"
    output_path = f"./include/processed/{execution_date}/"
    
    df = spark.read.parquet(input_path)
    
    # Procesar datos
    word_counts = extract_words(df)
    entities_df = extract_entities(df)    
    trends = analyze_trends(df)   
    news_sources = analyze_news_sources(df)
    
    # Guardar resultados en CSV
    if word_counts:
        word_counts.write.csv(output_path + "word_counts.csv", header=True, mode="overwrite")
        logger.info("Archivo 'word_counts.csv' guardado correctamente.")
    
    if entities_df:
        entities_df.write.csv(output_path + "entities.csv", header=True, mode="overwrite")
        logger.info("Archivo 'entities.csv' guardado correctamente.")
    
    if trends:
        trends.write.csv(output_path + "trends.csv", header=True, mode="overwrite")
        logger.info("Archivo 'trends.csv' guardado correctamente.")
    
    if news_sources:
        news_sources.write.csv(output_path + "news_sources.csv", header=True, mode="overwrite")
        logger.info("Archivo 'news_sources.csv' guardado correctamente.")
    
    logger.info("Proceso finalizado exitosamente.")
    spark.stop()

if __name__ == "__main__":
    main()
