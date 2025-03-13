import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, regexp_replace, to_date
import os

def clean(df):
    print("🔹 Aplicando limpieza de datos...")
    df = df.withColumn("summary", lower(trim(regexp_replace(col("summary"), "\\n", " ")))) \
           .withColumn("published_at", to_date(col("published_at"))) \
           .withColumn("updated_at", to_date(col("updated_at")))

    df.show(5, truncate=False)  # Verificar primeras filas después de limpieza
    return df

def deduplicate(df):
    print("🔹 Eliminando duplicados...")
    df = df.dropDuplicates(["id"])
    print(f"🔹 Registros después de deduplicar: {df.count()}")
    return df

def main():
    spark = SparkSession.builder \
        .appName("Clean and Deduplicate") \
        .master("local[*]") \
        .getOrCreate()
    
    # Configurar argumentos
    parser = argparse.ArgumentParser()
    parser.add_argument("--execution_date", required=True, help="Fecha de ejecución en formato YYYY-MM-DD")
    args = parser.parse_args()
    
    execution_date = args.execution_date
    print(f"📅 Procesando datos para la fecha: {execution_date}")    
    
    input_dir = f"./include/extract/{execution_date}"
    output_dir = f"./include/processed/{execution_date}"

    files = ["articles.csv", "reports.csv", "blogs.csv"]
    
    # Leer archivos CSV
    dfs = []
    for file in files:
        path = os.path.join(input_dir, file)
        if os.path.exists(path):  # Verifica si el archivo existe antes de leerlo
            print(f"🔹 Leyendo archivo: {file}")
            df = spark.read.csv(path, header=True, inferSchema=True)
            df.show(3, truncate=False)  # Muestra algunas filas del archivo leído
            dfs.append(df)
        else:
            print(f"⚠️ Archivo no encontrado: {file}")
    
    if not dfs:
        print("⚠️ No hay archivos para procesar. Finalizando.")
        spark.stop()
        return
    
    # Unir DataFrames
    df = dfs[0]
    for d in dfs[1:]:
        df = df.unionByName(d)
    
    print("🔹 DataFrame unificado:")
    df.printSchema()
    
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
    
    df = clean(df)
    df = deduplicate(df)

    # Guardar en Parquet
    parquet_path = os.path.join(output_dir, "documents_clean_deduplicate.parquet")
    print(f"🔹 Guardando datos en formato Parquet: {parquet_path}")
    df.write.mode("overwrite").partitionBy("published_at").parquet(parquet_path)
    
    # Guardar en CSV
    csv_path = os.path.join(output_dir, "documents_clean_deduplicate.csv")
    print(f"🔹 Guardando datos en formato CSV: {csv_path}")
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_path)

    print("✅ Proceso finalizado correctamente.")
    spark.stop()    

if __name__ == "__main__":
    main()
