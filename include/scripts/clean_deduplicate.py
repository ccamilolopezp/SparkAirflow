import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, regexp_replace, to_date
import logging

# Configuración del logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def clean(df):
    """ Aplica limpieza de datos en las columnas de interés. """
    logging.info("Aplicando limpieza de datos...")
    df = df.withColumn("summary", lower(trim(regexp_replace(col("summary"), "\\n", " ")))) \
           .withColumn("published_at", to_date(col("published_at"))) \
           .withColumn("updated_at", to_date(col("updated_at")))
    
    df.show(5, truncate=False)  # Vista previa de los datos después de la limpieza
    return df

def deduplicate(df):
    """ Elimina registros duplicados en base al campo 'id'. """
    logging.info("Eliminando registros duplicados...")
    df = df.dropDuplicates(["id"])
    logging.info(f"Total de registros después de deduplicar: {df.count()}")
    return df

def main():
    """ Punto de entrada del script para limpieza y deduplicación de datos. """
    spark = SparkSession.builder \
        .appName("Clean and Deduplicate") \
        .master("local[*]") \
        .getOrCreate()
    
    # Configuración de argumentos
    parser = argparse.ArgumentParser()
    parser.add_argument("--execution_date", required=True, help="Fecha de ejecución en formato YYYY-MM-DD")
    args = parser.parse_args()
    
    execution_date = args.execution_date
    logging.info(f"Procesando datos para la fecha: {execution_date}")
    
    input_dir = f"./include/extract/{execution_date}"
    output_dir = f"./include/processed/{execution_date}"
    os.makedirs(output_dir, exist_ok=True)  # Asegurar que el directorio de salida exista

    files = ["articles.csv", "reports.csv", "blogs.csv"]
    dfs = []
    
    # Leer archivos CSV
    for file in files:
        path = os.path.join(input_dir, file)
        if os.path.exists(path):
            logging.info(f"Leyendo archivo: {file}")
            df = spark.read.csv(path, header=True, inferSchema=True)
            df.show(3, truncate=False)  # Vista previa de los primeros registros
            dfs.append(df)
        else:
            logging.warning(f"Archivo no encontrado: {file}")
    
    if not dfs:
        logging.warning("No se encontraron archivos para procesar. Finalizando ejecución.")
        spark.stop()
        return
    
    # Unir DataFrames
    df = dfs[0]
    for d in dfs[1:]:
        df = df.unionByName(d)
    
    logging.info("Estructura del DataFrame unificado:")
    df.printSchema()
    
    # Selección y transformación de columnas
    df = df.selectExpr(
        "id",
        "title",
        "url",
        "image_url",
        "news_site",
        "summary",
        "published_at",
        "updated_at",
        "featured",
        "CAST(launches AS STRING) AS launches",
        "CAST(events AS STRING) AS events"
    )
    
    # Aplicar limpieza y deduplicación
    df = clean(df)
    df = deduplicate(df)
    
    # Guardar resultados en formato Parquet
    parquet_path = os.path.join(output_dir, "documents_clean_deduplicate.parquet")
    logging.info(f"Guardando datos en formato Parquet: {parquet_path}")
    df.write.mode("overwrite").partitionBy("published_at").parquet(parquet_path)
    
    # Guardar resultados en formato CSV
    csv_path = os.path.join(output_dir, "documents_clean_deduplicate.csv")
    logging.info(f"Guardando datos en formato CSV: {csv_path}")
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_path)
    
    logging.info("Proceso finalizado correctamente.")
    spark.stop()

if __name__ == "__main__":
    main()