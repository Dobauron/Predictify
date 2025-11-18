import os
import pandas as pd
import yfinance as yf
from datetime import datetime
from dotenv import load_dotenv


class ETFDataFetcher:
    # Mapping sektor → ticker ETF
    sector_etf_map = {
        "Technology": "XLK",
        "Communication Services": "XLC",
        "Financials": "XLF",
        "Real Estate": "XLRE",
        "Energy": "XLE",
        "Utilities": "XLU",
        "Industrials": "XLI",
        "Materials": "XLB",
        "Consumer Discretionary": "XLY",
        "Consumer Staples": "XLP",
        "Health Care": "XLV"
    }

    def __init__(self, start_year=None):
        self.start_year = start_year or (datetime.now().year - 10)
        self.data = pd.DataFrame()

    # ---------------------------
    # FILE CHECK
    # ---------------------------
    def is_file_up_to_date(self, path: str) -> bool:
        if not os.path.exists(path):
            return False
        try:
            df = pd.read_csv(path)
        except:
            return False

        if df.empty:
            return False

        latest_year_in_file = df["Year"].max()
        current_year = datetime.now().year
        current_quarter = (datetime.now().month - 1) // 3 + 1

        df_q = df[df["Year"] == latest_year_in_file]
        latest_quarter = df_q["Quarter"].max() if "Quarter" in df_q.columns else 0

        return (latest_year_in_file > current_year) or \
               (latest_year_in_file == current_year and latest_quarter >= current_quarter)

    # ---------------------------
    # FETCH PRICES
    # ---------------------------
    def fetch_prices(self):
        all_data = []
        start_date = f"{self.start_year}-01-01"
        end_date = datetime.now().strftime("%Y-%m-%d")

        for sector, ticker in self.sector_etf_map.items():
            print(f"Fetching {ticker} for {sector}...")
            etf = yf.download(ticker, start=start_date, end=end_date, progress=False)
            if etf.empty:
                print(f"⚠️ No data for {ticker}")
                continue

            etf = etf[["Close"]].rename(columns={"Close": "Price"})
            etf["Sector"] = sector
            etf["Ticker"] = ticker
            etf = etf.reset_index().rename(columns={"Date": "Date"})
            all_data.append(etf)

        if not all_data:
            raise RuntimeError("❌ No ETF data fetched.")
        self.data = pd.concat(all_data, ignore_index=True)
        return self.data

    # ---------------------------
    # CONVERT TO QUARTERLY RETURNS
    # ---------------------------
    def compute_quarterly_returns(self):
        if self.data.empty:
            raise RuntimeError("Data not fetched yet.")

        self.data["Year"] = self.data["Date"].dt.year
        self.data["Quarter"] = self.data["Date"].dt.quarter

        # keep last price of each quarter
        quarterly = self.data.groupby(["Sector", "Ticker", "Year", "Quarter"])["Price"].last().reset_index()

        # compute QoQ returns
        quarterly["Return_QoQ"] = quarterly.groupby("Sector")["Price"].pct_change()

        self.data = quarterly
        return self.data

    # ---------------------------
    # SAVE CSV
    # ---------------------------
    def save_csv(self, path: str):
        directory = os.path.dirname(path)
        if directory and not os.path.exists(directory):
            os.makedirs(directory)
        self.data.to_csv(path, index=False)
        print(f"[OK] Saved: {path}")

    # ---------------------------
    # LOAD OR UPDATE
    # ---------------------------
    def load_or_update(self, path: str):
        if self.is_file_up_to_date(path):
            print("✔ Local ETF file is up-to-date. Loading...")
            self.data = pd.read_csv(path)
            return self.data

        print("⚠ Local ETF file outdated or missing → downloading new data...")
        self.fetch_prices()
        self.compute_quarterly_returns()
        self.save_csv(path)
        return self.data


# ---------------------------
# EXAMPLE USAGE
# ---------------------------
if __name__ == "__main__":
    load_dotenv()
    fetcher = ETFDataFetcher(start_year=datetime.now().year - 10)
    df = fetcher.load_or_update("data/raw/etf/sector_etf_last10yrs_quarterly.csv")
    print(df.head())
