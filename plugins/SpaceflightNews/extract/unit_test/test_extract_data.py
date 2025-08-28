import unittest

from SpaceflightNews.extract.code.extract_data import extract_documents, fetch_data, get_api_info

class Test_Extract_Data(unittest.TestCase):
    
    def test_fetch_data(self):
        """Prueba que fetch_data retorna datos en formato dict y contiene 'results'."""
        data = fetch_data("articles", limit=1)
        self.assertIsInstance(data, dict)
        self.assertIn("results", data)
    
    def test_extract_documents(self):
        """Prueba que extract_documents retorna la ruta del archivo CSV generado."""
        file_path = extract_documents("articles", max_pages=1, limit=2)

        self.assertIsInstance(file_path, str)
        self.assertTrue(file_path.endswith(".csv"))

        import os
        self.assertTrue(os.path.exists(file_path))

        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertGreater(len(content), 0)
 

    def test_get_api_info(self):
        """Prueba que get_api_info retorna un diccionario con la versión de la API."""
        info = get_api_info()
        self.assertIsInstance(info, dict)
        self.assertIn("version", info) 