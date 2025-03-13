import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, count, desc, lower, array_join, collect_list, first
from pyspark.sql.window import Window
from pyspark.ml.feature import StopWordsRemover, Tokenizer, HashingTF, IDF
from pyspark.ml.clustering import KMeans

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

def classify_topics(df, num_clusters=5):
    """Clasifica artículos en temas usando KMeans y extrae palabras representativas."""
    if df is None or df.isEmpty():
        logger.warning("El DataFrame está vacío o es inválido.")
        return None, None

    # Tokenización de texto
    tokenizer = Tokenizer(inputCol="summary", outputCol="words")
    df = tokenizer.transform(df)

    # Eliminación de stopwords
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    df = remover.transform(df)

    # Vectorización TF-IDF
    hashingTF = HashingTF(inputCol="filtered_words", outputCol="rawFeatures", numFeatures=1000)
    df = hashingTF.transform(df)

    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idf_model = idf.fit(df)
    df = idf_model.transform(df)

    # Aplicar KMeans para agrupar en temas
    kmeans = KMeans(k=num_clusters, seed=42, featuresCol="features", predictionCol="topic_id")
    model = kmeans.fit(df)
    clustered_df = model.transform(df)

    # Extraer palabras clave por tema
    word_counts = clustered_df.withColumn("word", explode(col("filtered_words"))) \
                              .groupBy("topic_id", "word").count() \
                              .orderBy("topic_id", desc("count"))

    topic_keywords = word_counts.groupBy("topic_id") \
                                .agg(array_join(collect_list("word"), ", ").alias("keywords"))

    # Asociar un nombre y categoría de tema
    window_spec = Window.partitionBy("topic_id")
    topic_keywords = topic_keywords.withColumn("name", first(col("keywords")).over(window_spec))
    topic_keywords = topic_keywords.withColumn("category", col("name"))  # Temporalmente igual al nombre

    return clustered_df, topic_keywords.select("topic_id", "name", "category")

def main():
    spark = SparkSession.builder \
        .appName("Topic Detection") \
        .master("local[*]") \
        .getOrCreate()

    # Argumentos del script
    parser = argparse.ArgumentParser()
    parser.add_argument("--execution_date", required=True, help="Fecha de ejecución en formato YYYY-MM-DD")
    args = parser.parse_args()
    execution_date = args.execution_date

    logger.info(f"\U0001F4C5 Procesando detección de temas para la fecha: {execution_date}")

    input_path = f"./include/processed/{execution_date}/documents_clean_deduplicate.parquet"
    output_topics_path = f"./include/processed/{execution_date}/topics.csv"
    output_documents_path = f"./include/processed/{execution_date}/documents_topics.csv"

    # Cargar datos limpios
    df = spark.read.parquet(input_path)

    # Mantener la partición original
    df = df.repartition("published_at")

    # Clasificación de temas
    clustered_df, topics_df = classify_topics(df, num_clusters=5)

    if topics_df:
        topics_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_topics_path)
        logger.info(f"✅ Temas guardados en {output_topics_path}")

    if clustered_df:
        clustered_df = clustered_df.select("id", "topic_id")
        clustered_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_documents_path)
        logger.info(f"✅ Artículos con temas guardados en {output_documents_path}")

    spark.stop()

if __name__ == "__main__":
    main()
