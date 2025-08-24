import requests
import json
import pandas as pd

# Endpoint

url = '''
https://api.worldbank.org/v2/country/CZ/indicator/SL.UEM.TOTL.ZS?date=2000:2023&format=json&per_page=5000
'''

# Indicators

indicators = {
    "general_indicators" : {
        "total_unemployment_rate": "SL.UEM.TOTL.ZS",
        # "male_unemployment_rate": "SL.UEM.TOTL.MA.ZS",
        # "female_unemployment_rate": "SL.UEM.TOTL.FE.ZS",
    },
    # "age_indicators" : {
    #     "youth_unemployment_rate": "SL.UEM.1524.ZS",
    #     "youth_male_unemployment_rate": "SL.UEM.1524.MA.ZS",
    #     "youth_female_unemployment_rate": "SL.UEM.1524.FE.ZS"
    # },
    # "education_indicators" : {
    #     "unemployment_rate_basic_education_male": "SL.UEM.BASC.MA.ZS",
    #     "unemployment_rate_intermediate_education_male": "SL.UEM.INTM.MA.ZS",
    #     "unemployment_rate_advanced_education_male": "SL.UEM.ADVN.MA.ZS",
    #     "unemployment_rate_basic_education_female": "SL.UEM.BASC.FE.ZS",
    #     "unemployment_rate_intermediate_education_female": "SL.UEM.INTM.FE.ZS",
    #     "unemployment_rate_advanced_education_female": "SL.UEM.ADVN.FE.ZS",
    # }
}

# Date ranges
date_range_general = "2000:2023"
date_range_age = "2000:2023"
date_range_education = "2000:2023"

# Country codes
country_codes = {
    "Czech Republic": "CZ",
    "European Union": "EUU",
}



# Import
response = requests.get(url)

for indicator_text in indicators.values():
    for indicator_code in indicator_text.values():

        response = requests.get(
            f"https://api.worldbank.org/v2/country/{country_codes['Czech Republic']}/indicator/{indicator_code}?date={date_range_general}&format=json&per_page=5000"
        )
        if response.status_code == 200:
            data = response.json()

            records = data[1]

            df = pd.json_normalize(records)

            df = df.rename(
                columns={
                    'country.value': 'country',
                    'date': 'year',
                    'value': 'unemployment_rate'
                }
            )

            with open('data/raw/unemployment_CZ.json', 'w') as f:
                json.dump(data, f)

            df.to_csv('data/processed/unemployment_CZ.csv', index=False)

        else:
            print(f"Error: {response.status_code}")
