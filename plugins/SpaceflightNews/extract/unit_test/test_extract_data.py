import unittest

from SpaceflightNews.extract.code.extract_data import extract_documents, fetch_data, get_api_info

class Test_Extract_Data(unittest.TestCase):
    
    def test_fetch_data(self):
        """Prueba que fetch_data retorna datos en formato dict y contiene 'results'."""
        data = fetch_data("articles", limit=1)
        self.assertIsInstance(data, dict)
        self.assertIn("results", data)

    def test_get_api_info(self):
        """Prueba que get_api_info retorna un diccionario con la versión de la API."""
        info = get_api_info()
        self.assertIsInstance(info, dict)
        self.assertIn("version", info) 