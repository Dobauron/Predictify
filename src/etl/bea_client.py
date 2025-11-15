import os
import requests
import pandas as pd
from dotenv import load_dotenv

class BEAClient:
    """
    Klient do BEA API.
    - automatycznie wczytuje .env z root projektu
    - metody: get_data (DataFrame), save_to_csv
    """

    BASE_URL = "https://apps.bea.gov/api/data"

    def __init__(self):
        # Ścieżka do katalogu root projektu (2 poziomy w górę względem src/etl)
        self.root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
        dotenv_path = os.path.join(self.root_path, ".env")

        if not os.path.exists(dotenv_path):
            raise FileNotFoundError(f"Nie znaleziono pliku .env w katalogu root: {self.root_path}")

        load_dotenv(dotenv_path)

        self.api_key = os.getenv("BEA_API_KEY")
        if not self.api_key:
            raise ValueError("Brak BEA_API_KEY w pliku .env")

    def _make_request(self, params):
        response = requests.get(self.BASE_URL, params=params)
        if response.status_code != 200:
            raise ConnectionError(f"Błąd API BEA: {response.status_code} → {response.text}")
        data = response.json()
        if "BEAAPI" not in data or "Results" not in data["BEAAPI"]:
            raise ValueError(f"Niepoprawna odpowiedź API: {data}")
        return data

    def get_data(self, dataset: str, table_name: str, year: str = "ALL", extra_params: dict = None) -> pd.DataFrame:
        """
        Pobiera dane z BEA i zwraca DataFrame.

        :param dataset: np. "GDPbyIndustry"
        :param table_name: np. "VA"
        :param year: np. "ALL"
        :param extra_params: dodatkowe parametry API
        """
        params = {
            "UserID": self.api_key,
            "dataset": dataset,
            "TableName": table_name,
            "Year": year,
            "ResultFormat": "JSON"
        }

        if extra_params:
            params.update(extra_params)

        data = self._make_request(params)

        records = data
        df = pd.DataFrame(records)

        if "DataValue" in df.columns:
            df["DataValue"] = pd.to_numeric(df["DataValue"], errors="coerce")

        return df

    def save_to_csv(self, dataset: str, table_name: str, filename: str, year: str = "ALL"):
        """
        Pobiera dane i zapisuje do CSV.
        """
        if not os.path.isabs(filename):
            filename = os.path.join(self.root_path, filename)

        os.makedirs(os.path.dirname(filename), exist_ok=True)
        df = self.get_data(dataset, table_name, year)
        df.to_csv(filename, index=False)
        print(f"[BEA] Dane zapisane do: {filename}")


client = BEAClient()

df = client.get_data(
    datasetname="GDPbyIndustry",  # poprawna wielkość liter!
    table_name="VA",
    year="ALL"
)

print(df.head())
client.save_to_csv(
    datasetname="GDPbyIndustry",
    table_name="VA",
    filename="data/raw/value_added.csv"
)