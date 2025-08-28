import requests
import json
import pandas as pd
import time
import glob
import os

# Indicators

indicators = {
    "general_indicators" : {
        "total_unemployment_rate": "SL.UEM.TOTL.ZS",
        "male_unemployment_rate": "SL.UEM.TOTL.MA.ZS",
        "female_unemployment_rate": "SL.UEM.TOTL.FE.ZS",
    },
    "age_indicators" : {
        "youth_unemployment_rate": "SL.UEM.1524.ZS",
        "youth_male_unemployment_rate": "SL.UEM.1524.MA.ZS",
        "youth_female_unemployment_rate": "SL.UEM.1524.FE.ZS"
    },
    "education_indicators" : {
        "unemployment_rate_basic_education_male": "SL.UEM.BASC.MA.ZS",
        "unemployment_rate_intermediate_education_male": "SL.UEM.INTM.MA.ZS",
        "unemployment_rate_advanced_education_male": "SL.UEM.ADVN.MA.ZS",
        "unemployment_rate_basic_education_female": "SL.UEM.BASC.FE.ZS",
        "unemployment_rate_intermediate_education_female": "SL.UEM.INTM.FE.ZS",
        "unemployment_rate_advanced_education_female": "SL.UEM.ADVN.FE.ZS",
    }
}

# Date ranges
date_range_general = "2000:2023"
date_range_age = "2000:2023"
date_range_education = "2000:2023"

date_ranges = {
    'general': date_range_general,
    'age': date_range_age,
    'education': date_range_education
}

# Country codes
country_codes = {
    "Czech Republic": "CZ",
    "European Union": "EUU",
}

# Base path inside the container
BASE_PATH = '/opt/airflow/data'
RAW_PATH = os.path.join(BASE_PATH, 'raw')
PROCESSED_PATH = os.path.join(BASE_PATH, 'processed')
MERGED_PATH = os.path.join(BASE_PATH, 'merged')

# Ensure directories exist
os.makedirs(RAW_PATH, exist_ok=True)
os.makedirs(PROCESSED_PATH, exist_ok=True)
os.makedirs(MERGED_PATH, exist_ok=True)

# ETL Functions

def fetch_data_json(country_code, indicator_code, date_range):
    """
    Fetch raw data from the World Bank API for a given country and indicator.
    """
    url = f"https://api.worldbank.org/v2/country/{country_code}/indicator/{indicator_code}?date={date_range}&format=json&per_page=5000"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        return data[1]  # records
    else:
        print(f"Error {response.status_code} - {country_code} - {indicator_code}")
        return None

def transform_to_dataframe(records, indicator_name):
    """
    Convert API records into a pandas DataFrame.
    """
    if records is None:
        return None

    df = pd.json_normalize(records)
    df = df.rename(
        columns={
            'country.value': 'country',
            'date': 'year',
            'value': f'{indicator_name}'
        }
    )

    df = df[['country', 'year', f'{indicator_name}']]

    return df

def save_data(df, records, country_code, indicator_name):
    """
    Save raw JSON and processed CSV using absolute paths.
    """
    if df is None or records is None:
        return None

    json_file = os.path.join(RAW_PATH, f'unemployment_{country_code}_{indicator_name}.json')
    csv_file = os.path.join(PROCESSED_PATH, f'unemployment_{country_code}_{indicator_name}.csv')

    with open(json_file, 'w') as f:
        json.dump(records, f)

    df.to_csv(csv_file, index=False)
    print(f"Saved: {json_file} and {csv_file}")

def run_etl(country_codes, indicators, date_ranges):
    """
    Run ETL pipeline for multiple countries and indicators.
    """
    for country in country_codes.values():
        for category, indicator_text in indicators.items():
            date_range = (
                date_ranges['general'] if category == "general_indicators" else
                date_ranges['age'] if category == "age_indicators" else
                date_ranges['education']
            )
            for indicator_name, indicator_code in indicator_text.items():
                records = fetch_data_json(country, indicator_code, date_range)
                df = transform_to_dataframe(records, indicator_name)
                save_data(df, records, country, indicator_name)
                time.sleep(1)  # API rate limit

def merge_country_files(country_code, output_path=MERGED_PATH):
    files = glob.glob(os.path.join(PROCESSED_PATH, f"*_{country_code}_*.csv"))
    dfs = [pd.read_csv(f) for f in files]

    if not dfs:
        print(f"No processed files found for {country_code}")
        return None

    merged_df = dfs[0]
    for df in dfs[1:]:
        merged_df = pd.merge(merged_df, df, on=['country', 'year'], how='outer')

    merged_df = merged_df.sort_values(by='year')
    os.makedirs(output_path, exist_ok=True)
    merged_file = os.path.join(output_path, f"{country_code}_merged.csv")
    merged_df.to_csv(merged_file, index=False)

    print(f"Merged file saved: {merged_file}")
    return merged_df
