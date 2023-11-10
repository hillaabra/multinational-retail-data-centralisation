# %%
import json
import pandas as pd
import requests
# %%

import boto3
from botocore.exceptions import NoCredentialsError, ClientError, ParamValidationError # might not be needed
# %%
import sqlalchemy
import tabula
# %%
from database_utils import DatabaseConnector

# %%
class DataExtractor:

  # Method to extract database table to a pandas DataFrame, which it returns
  # Takes an instance of the DatabaseConnector class and the table name as an argument
  @staticmethod
  def read_rds_table(instance, table_name):
    engine = instance.init_db_engine()
    engine.connect()
    pd_df = pd.read_sql_table(table_name, engine)
    engine.dispose()
    return pd_df

  # method to extract data from a pdf document. takes link as argument. extracts all pages.
  # returns dataframe of extracted data
  @staticmethod
  def retrieve_pdf_data(link):

    dfs = tabula.read_pdf(link, lattice=True, pages='all', multiple_tables=False)[0]

    return dfs

  @staticmethod
  def retrieve_api_authorisation():
    with open('.secret/config.json', 'r') as read_file:
      header_dict = json.load(read_file)

    return header_dict

  # method to return the number of stores to extract from API
  def list_number_of_stores(self, endpoint, header_dict):

    response = requests.get(endpoint, headers=header_dict)

    if response.status_code == 200:

      return response

    else:

      print(response.status_code)

  # method to retrieve store data from API and return it in a dataframe
  def retrieve_stores_data(self, endpoint):

    header_dict = self.retrieve_api_authorisation()

    num_of_stores_response = self.list_number_of_stores("https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores", header_dict)
    num_of_stores = num_of_stores_response.json()['number_stores']

    response = requests.get(f"{endpoint}0", headers=header_dict)

    if response.status_code == 200:

      store_data = response.json()
      df_store_data = pd.DataFrame([store_data])
      df_store_data.set_index('index', inplace=True)

    else:

      print(response.status_code)

    for i in range(1, num_of_stores):

      response = requests.get(f"{endpoint}{i}", headers=header_dict)

      if response.status_code == 200:

        store_data = response.json()
        df_new_store_data = pd.DataFrame([store_data])
        df_store_data = pd.concat([df_store_data, df_new_store_data], ignore_index=True)

      else:

        print(response.status_code)

    return df_store_data

  # Method to extract products data from csv S3 storage, takes in S3 address ("s3://data-handling-public/products.csv") as an argument, returns pandas dataframe
  @staticmethod
  def extract_from_s3(s3_uri):

    s3_path_parts = s3_uri.split('://')[1].split('/')
    bucket_name = s3_path_parts[0]
    object_name = s3_path_parts[1]
    file_name = object_name

    s3 = boto3.client('s3')
    s3.download_file(bucket_name, object_name, file_name)

    products_df = pd.read_csv(file_name, index_col=[0])

    return products_df
    # also write code to remove csv file from project repo?
# %%
data_extractor = DataExtractor()
conn = DatabaseConnector()

def extract_card_data_for_cleaning():
  extracted_card_data = data_extractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
  return extracted_card_data

def extract_user_data_for_cleaning():
  extracted_user_data = data_extractor.read_rds_table(conn, 'legacy_users')
  return extracted_user_data

def extract_stores_data_for_cleaning():
  extracted_stores_data = data_extractor.retrieve_stores_data("https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/")
  return extracted_stores_data

def extract_products_data_for_cleaning():
  extracted_products_data = data_extractor.extract_from_s3("s3://data-handling-public/products.csv")
  return extracted_products_data

# conn.list_db_tables()

def extract_orders_data_for_cleaning():
  extracted_orders_data = data_extractor.read_rds_table(conn, 'orders_table')
  return extracted_orders_data
