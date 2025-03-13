import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, count, desc, array_join, collect_list, first, substring, expr
from pyspark.sql.window import Window
from pyspark.ml.feature import StopWordsRemover, Tokenizer, HashingTF, IDF
from pyspark.ml.clustering import KMeans

# Configuración de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()

def classify_topics(df, num_clusters=5):
    """Clasifica artículos en temas utilizando KMeans y extrae palabras clave representativas."""
    if df is None or df.isEmpty():
        logger.warning("El DataFrame de entrada está vacío o no es válido. No se procederá con la clasificación.")
        return None, None

    logger.info("Ejecutando tokenización de los textos de los artículos.")
    tokenizer = Tokenizer(inputCol="summary", outputCol="words")
    df = tokenizer.transform(df)

    logger.info("Eliminando palabras irrelevantes del contenido de los artículos.")
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    df = remover.transform(df)

    logger.info("Aplicando transformación de Frecuencia de Términos (TF).")
    hashingTF = HashingTF(inputCol="filtered_words", outputCol="rawFeatures", numFeatures=1000)
    df = hashingTF.transform(df)

    logger.info("Calculando la Frecuencia Inversa de Documentos (IDF).")
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idf_model = idf.fit(df)
    df = idf_model.transform(df)

    logger.info(f"Agrupando los artículos en {num_clusters} categorías mediante KMeans.")
    kmeans = KMeans(k=num_clusters, seed=42, featuresCol="features", predictionCol="topic_id")
    model = kmeans.fit(df)
    clustered_df = model.transform(df)

    logger.info("Extrayendo palabras clave representativas para cada tema identificado.")
    word_counts = clustered_df.withColumn("word", explode(col("filtered_words"))) \
                              .groupBy("topic_id", "word").count() \
                              .orderBy("topic_id", desc("count"))

    topic_keywords = word_counts.groupBy("topic_id") \
                                .agg(array_join(collect_list("word"), ", ").alias("keywords"))

    logger.info("Generando etiquetas descriptivas para cada tema detectado.")
    window_spec = Window.partitionBy("topic_id")
    topic_keywords = topic_keywords.withColumn("name", substring(first(col("keywords")).over(window_spec), 1, 255))

    most_frequent_word_df = word_counts.withColumn("rank", expr("ROW_NUMBER() OVER (PARTITION BY topic_id ORDER BY count DESC)")) \
                                       .filter(col("rank") == 1) \
                                       .select("topic_id", "word")

    topic_keywords = topic_keywords.join(most_frequent_word_df, "topic_id", "left") \
                                   .withColumnRenamed("word", "category") \
                                   .withColumn("category", substring(col("category"), 1, 255))

    logger.info("Proceso de clasificación de temas completado.")
    return clustered_df, topic_keywords.select("topic_id", "name", "category")

def main():
    """Punto de entrada principal del script para detección de temas en artículos."""
    logger.info("Inicializando la sesión de Spark.")
    spark = SparkSession.builder \
        .appName("Topic Detection") \
        .master("local[*]") \
        .getOrCreate()

    parser = argparse.ArgumentParser(description="Script para clasificación de temas en artículos de SpaceNews.")
    parser.add_argument("--execution_date", required=True, help="Fecha de ejecución en formato YYYY-MM-DD")
    args = parser.parse_args()
    execution_date = args.execution_date

    logger.info(f"Iniciando detección de temas para la fecha: {execution_date}")
    input_path = f"./include/processed/{execution_date}/documents_clean_deduplicate.parquet"
    output_topics_path = f"./include/processed/{execution_date}/topics.csv"
    output_documents_path = f"./include/processed/{execution_date}/documents_topics.csv"

    logger.info(f"Cargando datos preprocesados desde: {input_path}")
    df = spark.read.parquet(input_path)

    logger.info("Reparticionando datos por la fecha de publicación para optimizar procesamiento.")
    df = df.repartition("published_at")

    logger.info("Ejecutando clasificación de artículos en temas.")
    clustered_df, topics_df = classify_topics(df, num_clusters=5)

    if topics_df:
        logger.info(f"Guardando temas identificados en: {output_topics_path}")
        topics_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_topics_path)
        logger.info("Archivo de temas guardado correctamente.")

    if clustered_df:
        logger.info(f"Guardando asignación de temas para cada artículo en: {output_documents_path}")
        clustered_df = clustered_df.select("id", "topic_id")
        clustered_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_documents_path)
        logger.info("Archivo de asignación de artículos a temas guardado correctamente.")

    logger.info("Finalizando ejecución del script y cerrando la sesión de Spark.")
    spark.stop()

if __name__ == "__main__":
    main()
