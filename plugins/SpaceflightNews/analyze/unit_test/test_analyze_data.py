import unittest
from pyspark.sql import SparkSession
from SpaceflightNews.analyze.code.analyze_data import analyze_news_sources, analyze_trends, classify_articles, extract_entities, extract_words

spark = SparkSession.builder \
    .appName("SpaceflightNewsAnalysis") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

class Test_Analyze_Data(unittest.TestCase):
    
    def setUp(self):
        """Configura datos de prueba antes de cada test."""
        self.sample_articles = [
            {
                "id": "1",
                "title": "NASA Launches New Rocket",
                "url": "http://example.com/article1",
                "image_url": "http://example.com/image1.jpg",
                "news_site": "NASA News",
                "summary": "NASA successfully launched a new rocket to Mars.",
                "published_at": "2024-01-01",
                "updated_at": "2024-01-02",
                "featured": False,
                "launches": "[]",
                "events": "[]"
            },
            {
                "id": "2",
                "title": "SpaceX Announces Moon Mission",
                "url": "http://example.com/article2",
                "image_url": "http://example.com/image2.jpg",
                "news_site": "SpaceX News",
                "summary": "SpaceX is preparing for a new mission to the Moon.",
                "published_at": "2024-02-01",
                "updated_at": "2024-02-02",
                "featured": True,
                "launches": "[]",
                "events": "[]"
            }
        ]
        self.df = spark.createDataFrame(self.sample_articles)
    
    def test_extract_words(self):
        """Prueba que extract_words procesa correctamente el DataFrame y devuelve conteo de palabras."""
        word_counts = extract_words(self.df)
        self.assertIsNotNone(word_counts)
        self.assertGreater(word_counts.count(), 0)
    
    def test_extract_entities(self):
        """Prueba que extract_entities extrae entidades correctamente."""
        entities_df = extract_entities(self.sample_articles)
        self.assertIsNotNone(entities_df)
        self.assertGreater(entities_df.count(), 0)
    
    def test_classify_articles(self):
        """Prueba que classify_articles clasifica los artículos en clusters."""
        clustered_df = classify_articles(self.df, 2)
        self.assertIsNotNone(clustered_df)
        self.assertIn("cluster", clustered_df.columns)
    
    def test_analyze_trends(self):
        """Prueba que analyze_trends analiza correctamente las tendencias de publicación."""
        trends = analyze_trends(self.df)
        self.assertIsNotNone(trends)
        self.assertGreater(trends.count(), 0)
    
    def test_analyze_news_sources(self):
        """Prueba que analyze_news_sources cuenta correctamente las fuentes de noticias."""
        sources = analyze_news_sources(self.df)
        self.assertIsNotNone(sources)
        self.assertGreater(sources.count(), 0)   