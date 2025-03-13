import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, StructType, StructField, FloatType
from textblob import TextBlob   

def analyze_sentiment(text):
    """Determina el sentimiento de un texto usando TextBlob, retornando la categoría y el score."""
    analysis = TextBlob(text)
    polarity = analysis.sentiment.polarity
    sentiment = "positive" if polarity > 0 else "negative" if polarity < 0 else "neutral"
    return sentiment, polarity

def main():
    spark = SparkSession.builder \
        .appName("Sentiment Analysis") \
        .master("local[*]") \
        .getOrCreate()
    
    # Configurar argumentos
    parser = argparse.ArgumentParser()
    parser.add_argument("--execution_date", required=True, help="Fecha de ejecución en formato YYYY-MM-DD")
    args = parser.parse_args()

    execution_date = args.execution_date
    print(f"\U0001F4C5 Procesando datos para la fecha: {execution_date}")  
    
    input_path = f"./include/processed/{execution_date}/documents_clean_deduplicate.parquet"
    output_path = f"./include/processed/{execution_date}/sentiment_analysis.csv"
    
    # Cargar datos limpios
    df = spark.read.parquet(input_path)
    
    # Definir UDF para análisis de sentimiento con retorno múltiple
    sentiment_schema = StructType([
        StructField("sentiment", StringType(), False),
        StructField("sentiment_score", FloatType(), False)
    ])
    
    sentiment_udf = udf(analyze_sentiment, sentiment_schema)
    
    # Aplicar análisis de sentimientos
    df = df.withColumn("sentiment_data", sentiment_udf(col("summary")))
    
    # Expandir las columnas generadas por la UDF y seleccionar solo las necesarias
    df = df.withColumn("sentiment", col("sentiment_data.sentiment")) \
           .withColumn("sentiment_score", col("sentiment_data.sentiment_score")) \
           .drop("sentiment_data")  # Eliminar la columna intermedia
    
    df = df.select("id", "sentiment_score", "sentiment")
    
    # Guardar resultados en CSV
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
    
    spark.stop()

if __name__ == "__main__":
    main()
