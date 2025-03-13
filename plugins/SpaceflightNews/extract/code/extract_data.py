import csv
import logging
import os
import requests
import time
from datetime import datetime
from SpaceflightNews.parameters import BASE_URL, OUTPUT_EXTRACT_PATH, RATE_LIMIT

# Configuración de logging
logger = logging.getLogger("ExtractData")
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler("spaceflight_news.log", mode='w')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)
logger.addHandler(console_handler)


def fetch_data(endpoint, page=1, limit=10, date=None):
    """Obtiene datos paginados desde la API de Spaceflight News."""
    url = f"{BASE_URL}/{endpoint}/?limit={limit}&offset={(page-1) * limit}"
    if date:
        url += f"&published_at_gte={date}"
    
    logger.info(f"Consultando la API: {url}")
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Error en la solicitud a la API: {e}")
        return None


def extract_documents(endpoint, max_pages=5, limit=10, date=None):
    """Extrae y almacena artículos desde la API de Spaceflight News en formato CSV."""
    all_articles = []
    
    if date is None:
        date = datetime.today().strftime('%Y-%m-%d')
    
    output_dir = os.path.join(OUTPUT_EXTRACT_PATH, date)
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"{endpoint}.csv")
    
    logger.info(f"Iniciando extracción de datos para la fecha: {date}")
    
    for page in range(1, max_pages + 1):
        data = fetch_data(endpoint, page, limit, date)
        if not data or "results" not in data:
            logger.info("No se encontraron más datos para procesar.")
            break

        for item in data["results"]:
            formatted_article = {
                "id": int(item.get("id", 0)),
                "title": item.get("title", "").strip().replace("\n", " ").replace("\r", " "),
                "url": item.get("url", "").strip(),
                "image_url": item.get("image_url", "").strip(),
                "news_site": item.get("news_site", "").strip(),
                "summary": " ".join(item.get("summary", "").split()).replace('"', "'"),
                "published_at": item.get("published_at", ""),
                "updated_at": item.get("updated_at", ""),
                "featured": item.get("featured", False),
                "launches": str(item.get("launches", [])),
                "events": str(item.get("events", []))
            }
            all_articles.append(formatted_article)
        
        logger.info(f"Página {page} procesada correctamente. Esperando {RATE_LIMIT} segundos antes de la siguiente solicitud.")
        time.sleep(RATE_LIMIT)
    
    if all_articles:
        keys = all_articles[0].keys()
        with open(output_file, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=keys)
            writer.writeheader()
            writer.writerows(all_articles)
        
        logger.info(f"Datos almacenados en: {output_file}")
    else:
        logger.warning("No se encontraron artículos para almacenar.")
    
    return output_file


def get_api_info(page=1, limit=10):
    """Obtiene la metadata de la API de Spaceflight News."""
    return fetch_data("info", page, limit)
