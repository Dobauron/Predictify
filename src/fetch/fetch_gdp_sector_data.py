import os
import pandas as pd
import beaapi
from dotenv import load_dotenv
from datetime import datetime


class BEAGdpFetcher:
    VALID_FREQUENCIES = {"A", "Q"}

    # Mapowanie BEA TableID → nazwa kolumny
    table_map = {
        "1": "VA",
        "5": "VA_percentGDP",
        "15": "GO",
        "208": "Wages",
    }

    # Mapowanie Industry code → nazwa sektora
    industry_to_sector = {
        "11": "Energy",
        "21": "Energy",
        "212": "Energy",
        "22": "Utilities",
        "23": "Industrials",
        "31": "Materials",
        "32": "Materials",
        "33": "Materials",
        "44": "Consumer Discretionary",
        "45": "Consumer Discretionary",
        "50": "Communication Services",
        "51": "Technology",
        "52": "Financials",
        "53": "Real Estate",
        "62": "Health Care",
        "72": "Consumer Discretionary",
        "517": "Technology",
        "518": "Technology",
        "519": "Technology",
        "445": "Consumer Staples"
    }

    # Lista kodów dla głównych 11 sektorów
    main_sector_codes = list(industry_to_sector.keys())

    def __init__(self, api_key: str):
        self.api_key = api_key.strip()
        self._validate_api_key()
        self.data = pd.DataFrame()

    # ---------------------------
    # VALIDATION HELPERS
    # ---------------------------

    def _validate_api_key(self):
        if not self.api_key:
            raise ValueError("❌ Missing BEA API key. Check your .env file.")

    def _validate_frequency(self, frequency: str):
        if frequency not in self.VALID_FREQUENCIES:
            raise ValueError(f"❌ Invalid frequency {frequency}. Must be one of {self.VALID_FREQUENCIES}.")

    def _validate_output_path(self, path: str):
        directory = os.path.dirname(path)
        if directory and not os.path.exists(directory):
            print(f"⚠️ Directory does not exist, creating: {directory}")

    # ---------------------------
    # AUTOMATIC LAST 10 YEARS
    # ---------------------------

    def last_10_years(self):
        current_year = datetime.now().year
        start_year = current_year - 10
        return ",".join(str(y) for y in range(start_year, current_year + 1))

    # ---------------------------
    # FETCHING
    # ---------------------------

    def fetch_table(self, table_id: str, years: str, frequency: str = "Q"):
        self._validate_frequency(frequency)

        df = beaapi.get_data(
            self.api_key,
            datasetname="GDPbyIndustry",
            TableID=table_id,
            Industry=",".join(self.main_sector_codes),
            Year=years,
            Frequency=frequency,
        )

        df = pd.DataFrame(df)
        if df.empty:
            raise RuntimeError(f"❌ Empty dataset returned for TableID={table_id}")

        # rename column
        df = df.rename(columns={"DataValue": self.table_map[table_id]})
        # map industry code → sector name
        df["Sector"] = df["Industry"].map(self.industry_to_sector)

        # keep only relevant columns
        df = df[["Year", "Quarter", "Sector", self.table_map[table_id]]]

        return df

    def fetch_all(self, years: str, frequency: str = "Q"):
        merged = None
        for table_id in self.table_map.keys():
            df = self.fetch_table(table_id, years, frequency)
            if merged is None:
                merged = df
            else:
                merged = merged.merge(df, on=["Year", "Quarter", "Sector"], how="outer")

        self.data = merged
        return merged

    # ---------------------------
    # FILE CHECKING
    # ---------------------------

    def is_file_up_to_date(self, path: str, frequency: str = "Q") -> bool:
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

        if frequency == "A":
            return latest_year_in_file >= current_year

        if frequency == "Q":
            current_quarter = (datetime.now().month - 1) // 3 + 1
            df_q = df[df["Year"] == latest_year_in_file]
            if "Quarter" not in df_q.columns:
                return False
            latest_quarter = df_q["Quarter"].max()
            return (latest_year_in_file > current_year) or \
                   (latest_year_in_file == current_year and latest_quarter >= current_quarter)

        return False

    # ---------------------------
    # SAVE CSV
    # ---------------------------

    def save_csv(self, path: str):
        self._validate_output_path(path)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.data.to_csv(path, index=False)
        print(f"[OK] Saved: {path}")

    # ---------------------------
    # AUTO LOAD OR UPDATE
    # ---------------------------

    def load_or_update(self, path: str, frequency: str = "Q"):
        if self.is_file_up_to_date(path, frequency):
            print("✔ Local file is up-to-date. Loading...")
            self.data = pd.read_csv(path)
            return self.data

        print("⚠ Local file outdated or missing → downloading new data...")
        years = self.last_10_years()
        df = self.fetch_all(years, frequency)
        self.save_csv(path)
        return df


# ---------------------------
# EXAMPLE USAGE
# ---------------------------
if __name__ == "__main__":
    load_dotenv()
    BEA_KEY = os.getenv("BEA_API_KEY")

    fetcher = BEAGdpFetcher(api_key=BEA_KEY)

    df = fetcher.load_or_update(
        path="data/raw/sector/sector_gdp_last10yrs_quarterly.csv",
        frequency="Q"
    )

    print(df.head())
