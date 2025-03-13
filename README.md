# Spaceflight News Data Pipeline

[DOCUMENTO_TECNICO.pdf](https://github.com/user-attachments/files/19221857/DOCUMENTO_TECNICO.pdf)

## Descripción
Este proyecto implementa un pipeline de datos utilizando Apache Airflow, Spark y PostgreSQL para analizar tendencias en la industria espacial a partir de los datos de la API de Spaceflight News.

## Características principales
- **Ingesta diaria** de artículos, blogs y reportes.
- **Procesamiento con Spark** para limpieza, deduplicación, análisis de sentimiento y clasificación de artículos.
- **Almacenamiento en PostgreSQL** siguiendo un modelo dimensional optimizado.
- **Orquestación con Airflow** para la ejecución automática y programada del pipeline.
- **Monitoreo y manejo de fallos** con logs detallados y alertas.

## Tecnologías utilizadas
- [Astro](https://docs.astronomer.io/) (Apache Airflow en contenedores)
- [Apache Spark](https://spark.apache.org/)
- [PostgreSQL](https://www.postgresql.org/)
- [Docker](https://www.docker.com/)
- Python

## Instalación y configuración
### 1. Clonar el repositorio
```bash
 git clone https://github.com/ccamilolopezp/SparkAirflow.git
 cd SparkAirflow
```

### 2. Levantar el entorno con Docker
```bash
astro dev start
```
Esto iniciará los servicios de Airflow, Spark y PostgreSQL en contenedores Docker.

### 3. Configurar la base de datos
Antes de ejecutar el DAG, es necesario conectarse a la base de datos PostgreSQL que levanta Astro y ejecutar el script de creación de la base de datos.

Puedes conectarte a PostgreSQL utilizando herramientas como:
- **DBeaver**
- **pgAdmin**
- **psql (línea de comandos)**

#### Credenciales de acceso (definidas en el Docker Compose de Astro):
- **Host:** `localhost`
- **Puerto:** `5432`
- **Usuario:** `postgres`
- **Contraseña:** `postgres`
- **Base de datos:** `postgres`

#### Ejecutar script de creación de esquema
El archivo `create_tables.sql` se encuentra en la carpeta `data`. Una vez conectado a la base de datos, ejecutar:
```sql
\-- Abrir y ejecutar el script en el cliente SQL de tu elección
\i data/create_tables.sql
```

### 4. Acceder a la interfaz de Airflow
Una vez que el entorno esté corriendo, accede a la interfaz web de Airflow en:
```
http://localhost:8080
```
Credenciales por defecto:
- **Usuario:** `admin`
- **Contraseña:** `admin`

### 5. Ejecutar el DAG
En la interfaz de Airflow, activar y ejecutar el DAG `spaceflight_news_pipeline` para iniciar el proceso ETL.

![Imagen de WhatsApp 2025-03-12 a las 21 21 45_4b46d35b](https://github.com/user-attachments/assets/38546dc6-da8b-47a9-a2e3-a257b8ec42f6)


## Estructura del repositorio
```
SparkAirflow/
│── dags/                      # Definición de DAGs de Airflow
│   ├── spaceflight_news_dag.py
│── data/                      # Script SQL para crear tablas y queries de Analisis de tendencias
│── include/                   # Contiene los Jobs de Spark y las carpetas donde se almacenan los procesamientos en local
│── plugins/                   # Modulo de python para la extraccion y carga.
│── Dockerfile                 # Configuración de imagen Docker
│── requirements.txt           # Dependencias de Python
│── README.md                  # Documentación del proyecto
```

## Flujo del pipeline
1. **Extracción de datos** desde la API de Spaceflight News.
2. **Limpieza y deduplicación** con Spark.
3. **Clasificación de artículos** y análisis de sentimiento.
4. **Almacenamiento en PostgreSQL** con modelo dimensional optimizado.
5. **Generación de insights** y actualización de dashboards.

## Consideraciones adicionales
- Se implementó `ON CONFLICT` en PostgreSQL para manejar inserciones duplicadas.
- El pipeline maneja reintentos y validaciones de datos.
- Logs detallados en Airflow y almacenamiento en archivos locales.

## Contacto
Si tienes preguntas o sugerencias, no dudes en abrir un issue en el repositorio.

=====

Sobre ASTRO: Overview
========

Welcome to Astronomer! This project was generated after you ran 'astro dev init' using the Astronomer CLI. This readme describes the contents of the project, as well as how to run Apache Airflow on your local machine.

Project Contents
================

Your Astro project contains the following files and folders:

- dags: This folder contains the Python files for your Airflow DAGs. By default, this directory includes one example DAG:
    - `example_astronauts`: This DAG shows a simple ETL pipeline example that queries the list of astronauts currently in space from the Open Notify API and prints a statement for each astronaut. The DAG uses the TaskFlow API to define tasks in Python, and dynamic task mapping to dynamically print a statement for each astronaut. For more on how this DAG works, see our [Getting started tutorial](https://www.astronomer.io/docs/learn/get-started-with-airflow).
- Dockerfile: This file contains a versioned Astro Runtime Docker image that provides a differentiated Airflow experience. If you want to execute other commands or overrides at runtime, specify them here.
- include: This folder contains any additional files that you want to include as part of your project. It is empty by default.
- packages.txt: Install OS-level packages needed for your project by adding them to this file. It is empty by default.
- requirements.txt: Install Python packages needed for your project by adding them to this file. It is empty by default.
- plugins: Add custom or community plugins for your project to this file. It is empty by default.
- airflow_settings.yaml: Use this local-only file to specify Airflow Connections, Variables, and Pools instead of entering them in the Airflow UI as you develop DAGs in this project.

Deploy Your Project Locally
===========================

1. Start Airflow on your local machine by running 'astro dev start'.

This command will spin up 4 Docker containers on your machine, each for a different Airflow component:

- Postgres: Airflow's Metadata Database
- Webserver: The Airflow component responsible for rendering the Airflow UI
- Scheduler: The Airflow component responsible for monitoring and triggering tasks
- Triggerer: The Airflow component responsible for triggering deferred tasks

2. Verify that all 4 Docker containers were created by running 'docker ps'.

Note: Running 'astro dev start' will start your project with the Airflow Webserver exposed at port 8080 and Postgres exposed at port 5432. If you already have either of those ports allocated, you can either [stop your existing Docker containers or change the port](https://www.astronomer.io/docs/astro/cli/troubleshoot-locally#ports-are-not-available-for-my-local-airflow-webserver).

3. Access the Airflow UI for your local Airflow project. To do so, go to http://localhost:8080/ and log in with 'admin' for both your Username and Password.

You should also be able to access your Postgres Database at 'localhost:5432/postgres'.

Deploy Your Project to Astronomer
=================================

If you have an Astronomer account, pushing code to a Deployment on Astronomer is simple. For deploying instructions, refer to Astronomer documentation: https://www.astronomer.io/docs/astro/deploy-code/

Contact
=======

The Astronomer CLI is maintained with love by the Astronomer team. To report a bug or suggest a change, reach out to our support.

