import beaapi
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()
BEA_KEY = os.getenv("BEA_API_KEY")


pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)


gdp_data = beaapi.get_parameter_values(BEA_KEY, 'GDPbyIndustry', 'TableID')
table_map = {
    "1": "VA",                # Value Added
    "5": "VA_percentGDP",     # % Contribution to GDP
    "15": "GO",               # Gross Output
    "208": "Wages"            # Compensation / Wages
}
sector_data_table = pd.DataFrame()

for id, values in table_map.items():
    df = beaapi.get_data(
        BEA_KEY,
        datasetname='GDPbyIndustry',
        TableID=id,
        Industry='ALL',
        Year='2020,2021,2022,2023,2024',
        Frequency='A',
    )
    df = pd.DataFrame(df)
    df = df.rename(columns={"DataValue": values})

    if sector_data_table.empty:
        sector_data_table = df[["Year", "Industry", values]]
    else:
        sector_data_table = sector_data_table.merge(
            df[["Year", "Industry", values]],
            on=["Year", "Industry"],
            how="outer"
        )

print(sector_data_table)