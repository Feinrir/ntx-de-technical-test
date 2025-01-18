import os
import pandas as pd
import requests
import json


def extract_csv_data(file_path: str) -> pd.DataFrame:
    #read data from local csv
    return pd.read_csv(file_path)

def extract_json_api_data(api_url: str) -> pd.DataFrame:
    #read json data from api
    response = requests.get(api_url)
    if response.status_code == 200:
        return pd.DataFrame(json.loads(response.text))
    else:
        raise ValueError(f"API request failed with status code {response.status_code}")
    
def extract_products_data(config: dict) -> pd.DataFrame:
    #extract product data from csv and fallback to api if csv is not available
    csv_file_path = config['data_source']['primary_csv']['file_path_products']
    api_url = config['data_source']['secondary_json_api']['url_products']

    if os.path.exists(csv_file_path):
        print(f"CSV file found at {csv_file_path}. Extracting data...")
        return extract_csv_data(csv_file_path)
    else:
        print(f"CSV file not found. Falling back to JSON API.")
        return extract_json_api_data(api_url)

def extract_transactions_data(config: dict) -> pd.DataFrame:
    #extract tranasction data from csv and fallback to api if csv is not available
    csv_file_path = config['data_source']['primary_csv']['file_path_transactions']
    api_url = config['data_source']['secondary_json_api']['url_transactions']

    if os.path.exists(csv_file_path):
        print(f"CSV file found at {csv_file_path}. Extracting data...")
        return extract_csv_data(csv_file_path)
    else:
        print(f"CSV file not found. Falling back to JSON API.")
        return extract_json_api_data(api_url)
