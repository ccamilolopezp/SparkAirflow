import logging
import spacy
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, count, desc, lower, to_date, date_format
from pyspark.ml.feature import StopWordsRemover, Tokenizer, HashingTF, IDF
from pyspark.ml.clustering import KMeans

# Configuración de logging con salida a archivo y consola
logger = logging.getLogger()
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler("spaceflight_news.log", mode='w')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

logger.addHandler(file_handler)
logger.addHandler(console_handler)

spark = SparkSession.builder \
    .appName("SpaceflightNewsAnalysis") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

nlp = spacy.load("en_core_web_sm")

def extract_words(df):
    if not df:
        logger.warning("Df invalido") 
        return None
    df = df.withColumn("words", split(lower(col("summary")), " "))
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    df = remover.transform(df)
    df_filtered = df.select(explode(col("filtered_words")).alias("filtered_words"))
    return df_filtered.groupBy("filtered_words").count().orderBy("count", ascending=False)  

def extract_entities(df):
    if not df:
        logger.warning("Df invalido") 
        return None
    def extract_entities_from_text(text):
        doc = nlp(text)
        return [(ent.text, ent.label_) for ent in doc.ents]
    
    entities_rdd = spark.sparkContext.parallelize(df).map(lambda x: (x["id"], extract_entities_from_text(x["summary"])))
    return spark.createDataFrame(entities_rdd, ["id", "entities"])

def classify_articles(df, num_clusters=5):
    """Clasifica artículos en temas usando KMeans"""
    if not df:
        logger.warning("Df invalido")
        return None

    # Tokenización
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

    # Aplicar KMeans
    kmeans = KMeans(k=num_clusters, seed=42, featuresCol="features", predictionCol="cluster")
    model = kmeans.fit(df)
    clustered_df = model.transform(df)

    return clustered_df.select("title", "summary", "cluster")

def analyze_trends(df):
    if not df:
        logger.warning("Df invalido") 
        return None
    return df.groupBy("published_at").count().orderBy(desc("published_at"))

def analyze_news_sources(df):
    if not df:
        logger.warning("Df invalido") 
        return None
    return df.groupBy("news_site").count().orderBy(desc("count"))   

def analyze_articles_with_spark(articles):
    """Realiza análisis de contenido y tendencias con Spark."""
    if not articles:
        logger.warning("No hay artículos para analizar")
        return None, None, None, None

    df = spark.createDataFrame(articles)

    df = df.withColumn("published_at", to_date(col("published_at"), "yyyy-MM-dd"))  # Eliminar segundos y milisegundos
    df = df.repartition(5, "published_at")  # Particionar según la fecha de publicación

    # Caching global
    df.cache()      
    
    word_counts = extract_words(df)  
    word_counts.show()

    entities_df = extract_entities(articles)
    entities_df.show()
    
    trends = analyze_trends(df)
    trends.show()
    
    news_sources = analyze_news_sources(df)
    news_sources.show()

    classified_articles = classify_articles(df)
    classified_articles.show()

    return word_counts, entities_df, trends, news_sources, classified_articles
