import yfinance as yf
import pandas as pd
from pathlib import Path


class TechnicalDataLoader:
    """
    Klasa odpowiedzialna za pobieranie technicznych danych giełdowych (OHLCV)
    dla wielu spółek, z możliwością zapisu do plików CSV.
    """

    def __init__(self, tickers, start="2015-01-01", end="2025-01-01",
                 interval="1d", save_dir="EvaSto/data/technicals"):
        self.tickers = [ticker.upper() for ticker in tickers]
        self.start = start
        self.end = end
        self.interval = interval
        self.save_path = Path(save_dir)
        self.save_path.mkdir(parents=True, exist_ok=True)

    def download_one(self, ticker):
        """
        Pobiera dane dla jednej spółki i zwraca DataFrame.
        """
        print(f"📥 Pobieram dane dla {ticker}...")

        df = yf.download(
            ticker.upper(),
            start=self.start,
            end=self.end,
            interval=self.interval,
            auto_adjust=False,
            progress=False
        )

        if df.empty:
            print(f"⚠️ Brak danych dla {ticker}, pomijam.")
            return None

        df.dropna(inplace=True)
        print(df.head())
        return df

    def save_to_csv(self, ticker, df):
        """
        Zapisuje DataFrame do pliku CSV.
        """
        file_path = self.save_path / f"{ticker}.csv"
        df.to_csv(file_path)
        print(f"✅ Zapisano: {file_path} ({len(df)} rekordów)")

    def download_all(self):
        """
        Pobiera dane dla wszystkich spółek i zapisuje je do CSV.
        """
        print("🚀 Start pobierania danych technicznych...\n")

        for ticker in self.tickers:
            df = self.download_one(ticker)
            if df is not None:
                self.save_to_csv(ticker, df)

        print("\n🎯 Pobieranie zakończone.")
        return True


if __name__ == "__main__":
    TICKERS = [
        "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN",
        "META", "TSLA", "AVGO", "ADBE", "INTC"
    ]

    loader = TechnicalDataLoader(TICKERS)

    raw_data = loader.download_one("NVDA")
    loader.save_to_csv("NVDA",raw_data)
