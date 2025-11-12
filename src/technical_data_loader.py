import yfinance as yf
import pandas as pd
from pathlib import Path

def download_technical_data(tickers, start="2015-01-01", end="2025-01-01", save_dir="data/raw"):
    """
    Pobiera dane techniczne (OHLCV) dla listy spółek i zapisuje je do plików CSV.
    """
    save_path = Path(save_dir)
    save_path.mkdir(parents=True, exist_ok=True)

    for ticker in tickers:
        print(f"📥 Pobieram dane dla {ticker}...")
        df = yf.download(ticker, start=start, end=end)
        df.dropna(inplace=True)
        df.to_csv(save_path / f"{ticker}.csv")
        print(f"✅ Zapisano: {save_path / f'{ticker}.csv'} ({len(df)} rekordów)")

    print("\n🎯 Wszystkie dane zostały pobrane.")
    return True


if __name__ == "__main__":
    TICKERS = [
        "AAPL", "MSFT", "NVDA", "GOOGL", "AMZN",
        "META", "TSLA", "AVGO", "ADBE", "INTC"
    ]

    download_technical_data(TICKERS)
