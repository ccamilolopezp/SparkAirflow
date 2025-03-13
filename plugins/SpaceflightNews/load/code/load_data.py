import logging
import os
import numpy as np
import pandas as pd
from SpaceflightNews.parameters import PROCESSED_DATA_PATH
from airflow.hooks.postgres_hook import PostgresHook

# Configuración de logging con salida a archivo y consola
logger = logging.getLogger()
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler("spaceflight_news.log", mode='w')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

logger.addHandler(file_handler)
logger.addHandler(console_handler)

def read_csv(file_name, execution_date):
    """Lee un archivo CSV en un DataFrame de Pandas."""
    folder_path = os.path.join(PROCESSED_DATA_PATH, execution_date, file_name)
    if os.path.exists(folder_path) and os.path.isdir(folder_path):
        files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
        if files:
            file_path = os.path.join(folder_path, files[0])
            logger.info(f"Cargando archivo: {file_path}")
            return pd.read_csv(file_path)
    
    logger.warning(f"Archivo no encontrado: {file_name}")
    return None

def merge_dataframes(df_articles, df_sources, df_topics, df_sentiment):
    """Une los DataFrames de artículos, fuentes y temas en la estructura de fact_article."""
    if df_articles is None or df_sources is None or df_topics is None:
        logger.error("No se pueden cruzar los datos debido a archivos faltantes.")
        return None

    logger.info("Cruzando los datos...")

    # Unir con las fuentes
    df_articles = df_articles.merge(df_sources[['id', 'source_name']], on="id", how="left")
    
    # Unir con los temas
    df_articles = df_articles.merge(df_topics[['id', 'topic_id']], on="id", how="left")

    # Unir con analisis sentimientos
    df_articles = df_articles.merge(df_sentiment[['id', 'sentiment_score']], on="id", how="left")

    # Seleccionar columnas relevantes
    df_articles = df_articles.rename(columns={"id": "article_id"})
    df_articles = df_articles[['article_id', 'source_name', 'topic_id', 'published_at', 'sentiment_score']]
    
    return df_articles

def calculate_importance(df_articles, df_sources):
    """Calcula el importance_score basado en confiabilidad, sentimiento y cantidad de artículos."""
    if df_articles is None:
        return None

    logger.info("Calculando importance_score...")

    # Definir coeficientes de ponderación
    alpha, beta, gamma = 0.5, 0.3, 0.2

    # Contar artículos por tema
    topic_counts = df_articles['topic_id'].value_counts().to_dict()

    # Aplicar la fórmula de importancia
    df_articles['importance_score'] = df_articles.apply(
        lambda row: (
            alpha * (df_sources[df_sources['source_name'] == row['source_name']]['reliability_score'].values[0] if row['source_name'] in df_sources['source_name'].values else 0) +
            beta * row['sentiment_score'] +
            gamma * np.log(1 + topic_counts.get(row['topic_id'], 0))
        ), axis=1
    )

    return df_articles

def load_processed_data(execution_date):
    """Ejecuta el flujo de procesamiento de datos."""
    logger.info(f"Procesando datos para la fecha: {execution_date}")

    # Leer archivos necesarios
    df_articles = read_csv("documents_clean_deduplicate.csv", execution_date)
    df_sources = read_csv("documents_sources.csv", execution_date)
    df_topics = read_csv("documents_topics.csv", execution_date)
    df_sentiment = read_csv("sentiment_analysis.csv", execution_date)


    # Cruzar DataFrames
    df_fact_article = merge_dataframes(df_articles, df_sources, df_topics, df_sentiment)  
    
    output_dir = os.path.join(PROCESSED_DATA_PATH, execution_date, "documents_merged.csv")
    os.makedirs(output_dir, exist_ok=True)  

    output_path = os.path.join(output_dir, "documents_merged.csv")
    df_fact_article.to_csv(output_path, index=False)
    logger.info(f"documents_merged guardado en {output_path}") 

def generate_daily_insights(execution_date):
    df_fact_article = read_csv("documents_merged.csv", execution_date)
    df_dim_source = read_csv("sources.csv", execution_date)
    df_entities = read_csv("entities.csv", execution_date)

    # Calcular importance_score
    df_fact_article = calculate_importance(df_fact_article, df_dim_source)

    df_fact_article = df_fact_article.merge(df_entities[['id', 'entities']], right_on="id", left_on="article_id", how="left")

    # Guardar resultados
    output_dir = os.path.join(PROCESSED_DATA_PATH, execution_date, "fact_articles.csv")
    os.makedirs(output_dir, exist_ok=True)  
    output_path = os.path.join(output_dir, "fact_articles.csv")
    df_fact_article.to_csv(output_path, index=False)
    
    logger.info(f"fact_articles guardado en {output_path}")

def update_dashboards(execution_date):
    """Actualiza los dashboards cargando datos en PostgreSQL."""
    logger.info(f"Actualizando dashboards para la fecha: {execution_date}")

    # Cargar los datos procesados
    df_fact_article = read_csv("fact_articles.csv", execution_date)
    df_dim_source = read_csv("sources.csv", execution_date)
    df_topics = read_csv("topics.csv", execution_date)

    if df_fact_article is None or df_dim_source is None or df_topics is None:
        logger.error("No se pueden actualizar los dashboards debido a archivos faltantes.")
        return

    # Conectar a PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id="postgres_conn")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    logger.info("Cargando datos en PostgreSQL...")

    # Mapeo de source_name a source_id
    source_id_map = {}

    # Insertar/Actualizar en `dim_news_source`
    for _, row in df_dim_source.iterrows():
        cursor.execute(
            """
            SET search_path TO spacenews;

            INSERT INTO dim_news_source (name, url, reliability_score)
            VALUES (%s, %s, %s)
            ON CONFLICT (name) DO UPDATE 
            SET reliability_score = EXCLUDED.reliability_score
            RETURNING source_id;
            """,
            (row['source_name'], row['url'], row['reliability_score'])
        )
        source_id = cursor.fetchone()[0]
        source_id_map[row['source_name']] = source_id  # Guardamos el mapping
    
    conn.commit()  

    # Convertir source_name en source_id en df_fact_article
    for index, row in df_fact_article.iterrows():
        source_name = row['source_name']
        if source_name in source_id_map:
            df_fact_article.at[index, 'source_id'] = source_id_map[source_name]
        else:
            # Si el source_name no estaba en df_dim_source, obtenerlo de la DB
            cursor.execute(
                """
                SET search_path TO spacenews;
                
                SELECT source_id FROM dim_news_source WHERE name = %s;
                """,
                (source_name,) 
            )
            result = cursor.fetchone()
            if result:
                df_fact_article.at[index, 'source_id'] = result[0]
                source_id_map[source_name] = result[0]  # Guardamos para futuras referencias
            else:
                logger.warning(f"No se encontró source_id para {source_name}, se omitirá este artículo.")
                df_fact_article.drop(index, inplace=True)  # Omitimos el artículo sin source_id
  
    for _, row in df_topics.iterrows():
        cursor.execute(
            """
            SET search_path TO spacenews;

            INSERT INTO dim_topic (topic_id, name, category)
            VALUES (%s, %s, %s)
            ON CONFLICT (topic_id) DO NOTHING;
            """,
            (row['topic_id'], row['topic_name'], row['category'])
        )

    conn.commit() 
   
    for _, row in df_fact_article.iterrows():
        cursor.execute(
            """
            SET search_path TO spacenews;

            INSERT INTO fact_article (article_id, source_id, topic_id, published_at, sentiment_score, importance_score)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (article_id, published_at) DO UPDATE 
            SET sentiment_score = EXCLUDED.sentiment_score,
                importance_score = EXCLUDED.importance_score;
            """,
            (row['article_id'], row['source_id'], row['topic_id'], row['published_at'], row['sentiment_score'], row['importance_score'])
        )

    conn.commit() 

    # Cerrar conexión
    cursor.close()
    conn.close()

    logger.info("Actualización de dashboards completada.")


    


    
