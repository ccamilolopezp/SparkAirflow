# Imagen base con Astro Runtime para Airflow
FROM quay.io/astronomer/astro-runtime:12.4.0

# Copiar dependencias y actualizar pip
COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt

# Descargar modelo de NLP de spaCy
RUN python -m spacy download en_core_web_sm

# Cambiar a usuario root para instalar Java
USER root

# Instalar OpenJDK 17 y herramientas necesarias
RUN apt update && \
    apt-get install -y openjdk-17-jdk ant && \
    apt-get clean;

# Definir variable de entorno para Java
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
RUN export JAVA_HOME

# Volver al usuario por defecto de Astro
USER astro
