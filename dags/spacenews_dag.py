from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from SpaceflightNews.extract.code.extract_data import extract_documents, get_api_info
from SpaceflightNews.load.code.load_data import load_processed_data, generate_daily_insights, update_dashboards
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Función para obtener la fecha de ejecución del DAG
# Prioriza la fecha configurada manualmente en `dag_run.conf`, de lo contrario, usa la fecha programada

def get_execution_date(**kwargs):
    return kwargs["dag_run"].conf.get("execution_date", kwargs["ds"])

# Funciones de extracción de datos por tipo de documento
def extract_articles(**kwargs):
    return extract_documents("articles", date=get_execution_date(**kwargs))

def extract_blogs(**kwargs):
    return extract_documents("blogs", date=get_execution_date(**kwargs))

def extract_reports(**kwargs):
    return extract_documents("reports", date=get_execution_date(**kwargs))

def extract_api_info():
    return get_api_info()

# Funciones para la carga y análisis de datos
def load_processed_data_call(**kwargs):
    return load_processed_data(get_execution_date(**kwargs))

def generate_daily_insights_call(**kwargs):
    return generate_daily_insights(get_execution_date(**kwargs))

def update_dashboards_call(**kwargs):
    return update_dashboards(get_execution_date(**kwargs))

# Función de alerta en caso de fallo
def failure_alert(context):
    print(f"Task {context['task_instance'].task_id} ha fallado")

# Configuración de los argumentos por defecto del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=20),
    'on_failure_callback': failure_alert
}

# Definición del DAG
dag = DAG(
    'spacenews_pipeline',
    default_args=default_args,
    description='Pipeline de SpaceNews con extracción, procesamiento y análisis',
    schedule_interval=timedelta(days=1),
    catchup=False,
    params={"execution_date": "{{ ds }}"}
)

# Tareas de extracción de datos
extract_articles_task = PythonOperator(
    task_id='extract_articles',
    python_callable=extract_articles,
    provide_context=True,
    dag=dag
)

extract_blogs_task = PythonOperator(
    task_id='extract_blogs',
    python_callable=extract_blogs,
    provide_context=True,
    dag=dag
)

extract_reports_task = PythonOperator(
    task_id='extract_reports',
    python_callable=extract_reports,
    provide_context=True,
    dag=dag
)

extract_api_info_task = PythonOperator(
    task_id='extract_api_info',
    python_callable=extract_api_info,
    dag=dag
)

# Procesamiento en Spark
clean_and_deduplicate_task = SparkSubmitOperator(
    task_id='clean_and_deduplicate',
    application='./include/scripts/clean_deduplicate.py',
    conn_id='my_spark_conn',
    verbose=True,
    application_args=["--execution_date", "{{ dag_run.conf.execution_date | default(ds) }}"],
    dag=dag
)

perform_sentiment_analysis_task = SparkSubmitOperator(
    task_id='perform_sentiment_analysis',
    application='./include/scripts/sentiment_analysis.py',
    conn_id='my_spark_conn',
    verbose=True,
    application_args=["--execution_date", "{{ dag_run.conf.execution_date | default(ds) }}"],
    dag=dag
)

identify_topics_task = SparkSubmitOperator(
    task_id='identify_topics',
    application='./include/scripts/identify_topics.py',
    conn_id='my_spark_conn',
    verbose=True,
    application_args=["--execution_date", "{{ dag_run.conf.execution_date | default(ds) }}"],
    dag=dag
)

identify_sources_task = SparkSubmitOperator(
    task_id='identify_sources',
    application='./include/scripts/identify_sources.py',
    conn_id='my_spark_conn',
    verbose=True,
    application_args=["--execution_date", "{{ dag_run.conf.execution_date | default(ds) }}"],
    dag=dag
)

identify_insights_task = SparkSubmitOperator(
    task_id='identify_insights',
    application='./include/scripts/identify_insights.py',
    conn_id='my_spark_conn',
    verbose=True,
    application_args=["--execution_date", "{{ dag_run.conf.execution_date | default(ds) }}"],
    dag=dag
)

# Carga y análisis de datos
load_processed_data_task = PythonOperator(
    task_id='load_processed_data',
    python_callable=load_processed_data_call,
    provide_context=True,
    dag=dag
)

generate_daily_insights_task = PythonOperator(
    task_id='generate_daily_insights',
    python_callable=generate_daily_insights_call,
    provide_context=True,
    dag=dag
)

update_dashboards_task = PythonOperator(
    task_id='update_dashboards',
    python_callable=update_dashboards_call,
    provide_context=True,
    dag=dag
)

# Definir dependencias entre tareas
extract_api_info_task >> [extract_articles_task, extract_blogs_task, extract_reports_task]
[extract_articles_task, extract_blogs_task, extract_reports_task] >> clean_and_deduplicate_task
clean_and_deduplicate_task >> [perform_sentiment_analysis_task, identify_topics_task, identify_sources_task, identify_insights_task]
[perform_sentiment_analysis_task, identify_topics_task, identify_sources_task, identify_insights_task] >> load_processed_data_task
load_processed_data_task >> generate_daily_insights_task >> update_dashboards_task
