import json
import pandas as pd
import requests
import sqlalchemy
import tabula
from database_utils import DatabaseConnector

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



de = DataExtractor()
dc = DatabaseConnector()

# Use the read_rds_table method to extract the table containing user data and return a pandas DataFrame
user_data = de.read_rds_table(dc, 'legacy_users')

# Use the retrieve_pdf_data method to extract the table containing card_data from the pdf link
pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
card_data = de.retrieve_pdf_data(pdf_link)

# Use the retrieve_stores_data method to extract the store data from the API and return it as a dataframe
store_data = de.retrieve_stores_data("https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/")
