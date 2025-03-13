import logging
import unittest
from SpaceflightNews.analyze.code.analyze_data import analyze_articles_with_spark
from SpaceflightNews.analyze.unit_test.test_analyze_data import Test_Analyze_Data
from SpaceflightNews.extract.code.extract_data import extract_documents, get_api_info
from SpaceflightNews.extract.unit_test.test_extract_data import Test_Extract_Data

# Configuración de logging con salida a archivo y consola
logger = logging.getLogger()
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler("spaceflight_news.log", mode='w')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

logger.addHandler(file_handler)
logger.addHandler(console_handler)        

if __name__ == "__main__":
    all_extracted_data = []  # Lista para acumular todos los artículos

    for endpoint in ["articles", "blogs", "reports"]:
        logger.info(f"Extrayendo datos de {endpoint}...")
        extracted_data = extract_documents(endpoint, 1, 10)
        if extracted_data:
            all_extracted_data.extend(extracted_data)  # Acumular datos en la lista

        logger.info(f"Se obtuvieron {len(extracted_data)} registros de {endpoint}.")

    # Obtener metadata de la API
    api_info = get_api_info(1, 10)
    logger.info(f"Información de la API: {api_info}")

    if all_extracted_data:
        analyze_articles_with_spark(all_extracted_data)
        
    print("\n=== Ejecutando pruebas unitarias ===\n")    
    unittest.TextTestRunner().run(unittest.TestLoader().loadTestsFromTestCase(Test_Extract_Data))
    unittest.TextTestRunner().run(unittest.TestLoader().loadTestsFromTestCase(Test_Analyze_Data))