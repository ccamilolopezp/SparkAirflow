# Spaceflight News Data Pipeline
Documento Técnico:
[DOCUMENTO_TECNICO.pdf](https://github.com/user-attachments/files/19221857/DOCUMENTO_TECNICO.pdf)

## Índice
1. [Descripción del Proyecto](#descripción-del-proyecto)
2. [Arquitectura](#arquitectura)
3. [Tecnologías Utilizadas](#tecnologías-utilizadas)
4. [Instalación y Configuración](#instalación-y-configuración)
5. [Estructura del Repositorio](#estructura-del-repositorio)
6. [Flujo del Pipeline](#flujo-del-pipeline)
7. [Consideraciones Adicionales](#consideraciones-adicionales)
8. [Contacto](#contacto)
9. [Créditos](#créditos)

## Descripción del Proyecto
Este proyecto implementa un pipeline de datos utilizando Apache Airflow, Spark y PostgreSQL para analizar tendencias en la industria espacial a partir de los datos de la API de Spaceflight News. La información se procesa y almacena en un modelo dimensional en PostgreSQL, permitiendo análisis de tendencias, clasificación por temas y validación de datos.

## Arquitectura
El pipeline consta de los siguientes componentes:
- **Ingesta de Datos**: Extracción diaria de artículos y eventos de la API de Spaceflight News.
- **Procesamiento**: Uso de Apache Spark para limpieza y transformación de datos.
- **Almacenamiento**: Carga de datos en PostgreSQL con particionamiento e índices.
- **Orquestación**: Apache Airflow gestiona la ejecución y dependencias entre tareas.
- **Monitoreo y Alertas**: Manejo de fallos y notificaciones.

### Diagrama Arquitectura:
[Diagrama Arquitectura.pdf](https://github.com/user-attachments/files/19221956/Diagrama.Arquitectura.pdf)
![image](https://github.com/user-attachments/assets/80c46fa0-c04d-485a-bc6f-4fa8038df5b7)

## Tecnologías Utilizadas
- **[Apache Airflow](https://airflow.apache.org/)** para orquestación.
- **[Astro CLI](https://www.astronomer.io/docs/astro/cli/overview/)** (contiene Airflow).
- **[Apache Spark](https://spark.apache.org/)** para procesamiento de datos.
- **[PostgreSQL](https://www.postgresql.org/)** para almacenamiento.
- **[Docker](https://www.docker.com/)** y **[Docker Compose](https://docs.docker.com/compose/)** para contenerización.
- **[Python](https://www.python.org/)** para scripting.

## Instalación y configuración

### Requisitos Previos
- Docker y Docker Compose instalados.
- Python 3.8+ instalado.
- Astro CLI instalado.
  
### 1. Clonar el repositorio
```bash
 git clone https://github.com/ccamilolopezp/SparkAirflow.git
 cd SparkAirflow
```

### 2. Levantar el entorno con Docker
```bash
astro dev start --wait 2m
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

## Estructura del Repositorio
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
3. **Extracción de artículos, fuentes e insights** y análisis de sentimiento.
4. **Merge de datos** y preparacion para actualizar en BD.
5. **Almacenamiento en PostgreSQL** con modelo dimensional optimizado.

## Consideraciones adicionales
- Se implementó `ON CONFLICT` en PostgreSQL para manejar inserciones duplicadas.
- El pipeline maneja reintentos y validaciones de datos.
- Logs detallados en Airflow y almacenamiento en archivos locales.

## Contacto
Si tienes preguntas o sugerencias, no dudes en abrir un issue en el repositorio.

## Créditos
Desarrollado por [Camilo López](https://www.linkedin.com/in/ccamilolopezp/). Inspirado en la API de Spaceflight News y tecnologías open-source.
