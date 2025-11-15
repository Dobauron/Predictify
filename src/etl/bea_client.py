import os
import requests
import pandas as pd
from dotenv import load_dotenv
from typing import Optional, Dict, Any


class BEAClient:
    """
    Klient do komunikacji z API Bureau of Economic Analysis (BEA).
    Ładuje klucz API z pliku .env i udostępnia metody pobierania danych.

    Przykład użycia:
        client = BEAClient()
        df = client.get_data(dataset="GDPbyIndustry", table_name="VA", year="ALL")
    """

    BASE_URL = "https://apps.bea.gov/api/data"

    def __init__(self, env_path: str = ".env"):
        """
        Inicjalizacja klienta BEA – ładowanie klucza API z pliku .env

        :param env_path: Ścieżka do pliku .env
        """
        load_dotenv(env_path)

        self.api_key = os.getenv("BEA_API_KEY")

        if not self.api_key:
            raise ValueError("Brak BEA_API_KEY w pliku .env")

    def _make_request(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Wysyła zapytanie GET do BEA API.

        :param params: Parametry zapytania
        :return: JSON jako dict
        """
        response = requests.get(self.BASE_URL, params=params)

        if response.status_code != 200:
            raise ConnectionError(
                f"Błąd zapytania BEA API: {response.status_code} -> {response.text}"
            )

        data = response.json()

        # Walidacja kluczy odpowiedzi
        if "BEAAPI" not in data or "Results" not in data["BEAAPI"]:
            raise ValueError(f"Niepoprawna odpowiedź API: {data}")

        return data

    def get_data(
        self,
        dataset: str,
        table_name: str,
        year: str = "ALL",
        params_extra: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """
        Pobiera dane BEA i zwraca DataFrame.

        :param dataset: Nazwa datasetu, np. "GDPbyIndustry"
        :param table_name: Nazwa tabeli, np. "VA"
        :param year: Rok lub "ALL"
        :param params_extra: Dodatkowe parametry API
        :return: Pandas DataFrame z wynikami
        """
        params = {
            "UserID": self.api_key,
            "dataset": dataset,
            "TableName": table_name,
            "Year": year,
            "ResultFormat": "JSON"
        }

        if params_extra:
            params.update(params_extra)

        data = self._make_request(params)

        try:
            records = data["BEAAPI"]["Results"]["Data"]
        except KeyError:
            raise ValueError(f"Nie udało się sparsować odpowiedzi: {data}")

        df = pd.DataFrame(records)

        # Zamiana DataValue na liczbę
        if "DataValue" in df.columns:
            df["DataValue"] = pd.to_numeric(df["DataValue"], errors="coerce")

        return df

    def save_to_csv(
        self,
        dataset: str,
        table_name: str,
        filename: str,
        year: str = "ALL"
    ) -> None:
        """
        Pobiera dane z BEA i zapisuje do pliku CSV.

        :param dataset: Dataset, np. "GDPbyIndustry"
        :param table_name: TableName, np. "VA"
        :param filename: Nazwa pliku wynikowego
        :param year: Rok lub "ALL"
        """
        df = self.get_data(dataset, table_name, year)
        df.to_csv(filename, index=False)
        print(f"[BEA] Dane zapisane do: {filename}")
