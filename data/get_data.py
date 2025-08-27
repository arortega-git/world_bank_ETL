import requests
import json
import pandas as pd
import time
import glob
import os

# Dependencies
os.makedirs('data/raw', exist_ok=True)
os.makedirs('data/processed', exist_ok=True)

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

# Import
for country in country_codes.values():
    for category, indicator_text in indicators.items():

        date_range = (
            date_range_general if category == "general_indicators" else
            date_range_age if category == "age_indicators" else
            date_range_education
        )

        for indicator_name, indicator_code in indicator_text.items():

            response = requests.get(
                f"https://api.worldbank.org/v2/country/{country}/indicator/{indicator_code}?date={date_range}&format=json&per_page=5000"
            )

            if response.status_code == 200:

                data = response.json()

                records = data[1]

                df = pd.json_normalize(records)

                df = df.rename(
                    columns={
                        'country.value': 'country',
                        'date': 'year',
                        'value': f'{indicator_name}'
                    }
                )
                
                file_name = f'unemployment_{country}_{indicator_name}'

                with open(
                    f'data/raw/{file_name}.json'
                    , 'w'
                    ) as f:
                    json.dump(data, f)

                df.to_csv(
                    f'data/processed/{file_name}.csv'
                    , index=False
                )

                time.sleep(1)

            else:
                print(f"Error {response.status_code} - {country} - {indicator_name}")

# ETL Functions

def fetch_data_json(country_code, indicator_code, date_range):
    """
    Fetch raw data from the World Bank API for a given country and indicator.
    
    Args:
        country_code (str): ISO code of the country (e.g., 'CZ', 'EUU').
        indicator_code (str): World Bank indicator code (e.g., 'SL.UEM.TOTL.ZS').
        date_range (str): Date range in format 'YYYY:YYYY' (e.g., '2000:2023').

    Returns:
        list or None: List of records (dictionaries) from API response, or None if request fails.
    
    Usage:
        records = fetch_data_json('CZ', 'SL.UEM.TOTL.ZS', '2000:2023')
    """
    response = requests.get(
        f"https://api.worldbank.org/v2/country/{country_code}/indicator/{indicator_code}?date={date_range}&format=json&per_page=5000"
    )

    if response.status_code == 200:
        data = response.json()
        records = data[1]
        return records
    else:
        print(f"Error {response.status_code} - {country_code} - {indicator_code}")
        return None
    

def transform_to_dataframe(records, indicator_name):
    """
    Convert a list of World Bank API records into a pandas DataFrame.
    
    Args:
        records (list): List of dictionaries returned by fetch_data_json.
        indicator_name (str): Name to assign to the 'value' column in the DataFrame.

    Returns:
        pd.DataFrame or None: DataFrame with columns 'country', 'year', indicator_name,
                              or None if input records is None.
    
    Usage:
        df = transform_to_dataframe(records, 'total_unemployment_rate')
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
    return df


def save_data(df, records, country_code, indicator_name):
    """
    Save both raw JSON and processed CSV data to disk.
    
    Args:
        df (pd.DataFrame): DataFrame returned by transform_to_dataframe.
        records (list): Raw records returned by fetch_data_json.
        country_code (str): ISO code of the country.
        indicator_name (str): Name of the indicator.

    Returns:
        None

    Side Effects:
        - Saves JSON to 'data/raw/unemployment_<country>_<indicator>.json'
        - Saves CSV to 'data/processed/unemployment_<country>_<indicator>.csv'
        - Prints confirmation message.

    Usage:
        save_data(df, records, 'CZ', 'total_unemployment_rate')
    """
    if df is None or records is None:
        return None

    file_name = f'unemployment_{country_code}_{indicator_name}'

    # Save raw JSON
    with open(f'data/raw/{file_name}.json', 'w') as f:
        json.dump(records, f)

    # Save processed CSV
    df.to_csv(f'data/processed/{file_name}.csv', index=False)
    
    print(f"{file_name} saved!")


def run_etl(country_codes, indicators, date_ranges):
    """
    Run the full ETL pipeline: fetch, transform, and save data for multiple countries and indicators.
    
    Args:
        country_codes (dict): Dictionary mapping country names to ISO codes.
        indicators (dict): Dictionary of indicator categories and codes.
        date_ranges (dict): Dictionary with date ranges for 'general', 'age', 'education' categories.

    Returns:
        None

    Usage:
        run_etl(country_codes, indicators, date_ranges)
    
    Notes:
        - Includes a 1-second sleep between requests to avoid hitting API rate limits.
        - Modular design allows each step to be easily converted into Airflow tasks.
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

                time.sleep(1)  # To respect API rate limits

def merge_country_files(country_code, output_path='data/merged/'):
    """
    Merge all processed CSV files for a given country into a single DataFrame.

    Args:
        country_code (str): The country code used in file naming (e.g. 'CZ', 'EUU').
        output_path (str): Directory where the merged file will be saved.

    Returns:
        pd.DataFrame: Merged DataFrame with all indicators for the country.
    """

    # Match all processed CSVs for this country
    files = glob.glob(f"data/processed/*_{country_code}_*.csv")

    dfs = [pd.read_csv(f) for f in files]

    if not dfs:
        print(f"No processed files found for {country_code}")
        return None

    merged_df = dfs[0]
    for df in dfs[1:]:
        merged_df = pd.merge(
            merged_df, df,
            on=['country', 'year'],
            how='outer'
        )

    merged_df = merged_df.sort_values(by='year')

    # Save merged file
    os.makedirs(output_path, exist_ok=True)
    merged_file = os.path.join(output_path, f"{country_code}_merged.csv")
    merged_df.to_csv(merged_file, index=False)

    print(f"Merged file saved: {merged_file}")
    return merged_df
