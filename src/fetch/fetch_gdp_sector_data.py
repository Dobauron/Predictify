import os
import pandas as pd
import beaapi
from dotenv import load_dotenv
from datetime import datetime


class BEAGdpFetcher:
    VALID_FREQUENCIES = {"A", "Q"}  # allowed

    def __init__(self, api_key: str):
        self.api_key = api_key.strip()
        self._validate_api_key()

        # Mapping BEA TableID -> column name
        self.table_map = {
            "1": "VA",
            "5": "VA_percentGDP",
            "15": "GO",
            "208": "Wages",
        }

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
    # 1) AUTOMATIC LAST 10 YEARS
    # ---------------------------

    def last_10_years(self):
        current_year = datetime.now().year
        start_year = current_year - 10
        return ",".join(str(y) for y in range(start_year, current_year + 1))

    # ---------------------------
    # 2) FETCHING
    # ---------------------------

    def fetch_table(self, table_id, years, frequency):
        df = beaapi.get_data(
            self.api_key,
            datasetname="GDPbyIndustry",
            TableID=table_id,
            Industry="ALL",
            Year=years,
            Frequency=frequency,
        )
        df = pd.DataFrame(df)
        if df.empty:
            raise RuntimeError(f"❌ Empty dataset returned for TableID={table_id}")

        df = df.rename(columns={"DataValue": self.table_map[table_id]})

        return df[["Year", "Industry", self.table_map[table_id]]]

    def fetch_all(self, years, frequency="Q"):
        self._validate_frequency(frequency)

        merged = None
        for table_id in self.table_map.keys():
            df = self.fetch_table(table_id, years, frequency)
            merged = df if merged is None else merged.merge(df, on=["Year", "Industry"], how="outer")

        self.data = merged
        return merged

    # ---------------------------
    # 3) LOCAL FILE CHECKING
    # ---------------------------

    def is_file_up_to_date(self, path: str, frequency: str) -> bool:
        """Checks whether a local CSV contains the newest year/quarter."""
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

        # yearly case
        if frequency == "A":
            return latest_year_in_file >= current_year

        # quarterly case
        if frequency == "Q":
            # check for current quarter (approx by month)
            current_quarter = (datetime.now().month - 1) // 3 + 1

            df_q = df[df["Year"] == latest_year_in_file]
            if "Quarter" not in df_q.columns:
                return False

            latest_quarter = df_q["Quarter"].max()

            return (latest_year_in_file > current_year) or (
                latest_year_in_file == current_year and latest_quarter >= current_quarter
            )

        return False

    # ---------------------------
    # 4) SAVE CSV
    # ---------------------------

    def save_csv(self, path: str):
        self._validate_output_path(path)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.data.to_csv(path, index=False)
        print(f"[OK] Saved: {path}")

    # ---------------------------
    # 5) AUTO FETCH OR LOAD OLD
    # ---------------------------

    def load_or_update(self, path: str, frequency="Q"):
        """Loads local CSV if up-to-date, otherwise downloads missing years."""
        self._validate_frequency(frequency)

        if self.is_file_up_to_date(path, frequency):
            print("✔ Local file is up-to-date. Loading...")
            self.data = pd.read_csv(path)
            return self.data

        print("⚠ Local file outdated or missing → downloading new data...")

        years = self.last_10_years()
        df = self.fetch_all(years, frequency)
        self.save_csv(path)

        return df


# --------------------------------------------------------
# EXAMPLE USAGE
# --------------------------------------------------------
if __name__ == "__main__":
    load_dotenv()
    BEA_KEY = os.getenv("BEA_API_KEY")

    fetcher = BEAGdpFetcher(api_key=BEA_KEY)

    df = fetcher.load_or_update(
        path="data/raw/sector/sector_gdp_last10yrs_quarterly.csv",
        frequency="Q"
    )

    print(df.head())
