import json
import os.path
import requests

from abc import ABC, abstractmethod

import boto3
from botocore.exceptions import ClientError
import pandas as pd
import sqlalchemy
import tabula

from database_utils import RDSDatabaseConnector

class DataExtractor(ABC):

    def __init__(self, source_location, extracted_data=None) -> None:

        self._source_location = source_location # this could be the name of the table (e.g. orders_table for RDS resource)
        self._extracted_data = extracted_data    # or the URI or URL or filepath etc...  (call this source_id instead?)

    @abstractmethod
    def extract_data(self):
       pass
    # play around with this.... maybe best to categorise by location first then type...
        # if self._source_type == 'pdf':
        #     df = self._retrieve_pdf_data(self._source_location)
        # elif self._source_type == 'JSON':
        #     df = self._extract_data_from_json_url(self._source_location)
        # elif self._source_type == 'AWS_RDS_resource':
        #     conn = RDSDatabaseConnector()
        #     df = self._read_rds_table(conn, self._source_location)
        # elif self._source_type == 'S3_resource':
        #     df = self._extract_from_s3(self._source_location)
        # elif self._source_type == 'api_resource':
        #     df = self._retrieve_stores_data(self._source_location)
        # self._extracted_data = df

    @staticmethod
    def _retrieve_pdf_data(pdf_url) -> pd.DataFrame:

      df = tabula.read_pdf(pdf_url, lattice=True, pages='all', multiple_tables=False)[0]

      return df

    @staticmethod
    def _extract_data_from_json_url(json_url) -> pd.DataFrame:

        df = pd.read_json(json_url)

        return df

    @staticmethod
    def _read_rds_table(rds_db_connector_instance, table_name) -> pd.DataFrame:
      engine = rds_db_connector_instance._init_db_engine()
      engine.connect()
      df = pd.read_sql_table(table_name, engine)
      engine.dispose()
      return df

    @staticmethod
    def _extract_from_s3(s3_uri) -> pd.DataFrame:

      s3_path_parts = s3_uri.split('://')[1].split('/')
      bucket_name = s3_path_parts[0]
      object_name = s3_path_parts[1]
      file_name = object_name

      # if object has already been downloaded, give option of using file already in repo instead of downloading again
      if os.path.isfile(file_name):

         user_input = f'File with name {file_name} already exists in repo.\
                        Enter 0 to extract the data from this local file. Enter 1 to delete the local file of this name\
                        and extract the data from the original S3 URI.'

         while user_input not in ['0', '1']:
            user_input = 'That was not a valid response. Enter the digit 0 to extract the data\
                        from the file already in the repo. Enter the digit 1 to delete the existing file\
                        and extract the data from the S3 URI.'

         if user_input == '0':

            products_df = pd.read_csv(file_name, index_col=[0])
            return products_df

         elif user_input == '1':

            os.remove(file_name)

            try:

              s3 = boto3.client('s3')
              s3.download_file(bucket_name, object_name, file_name)
              products_df = pd.read_csv(file_name, index_col=[0])
              # also write code to remove csv file from project repo?
              return products_df

            except ClientError as e:

              if e.response['Error']['Code'] == 'NoSuchBucket':
                print('The specified bucket does not exist.')
              elif e.response['Error']['Code'] == 'NoSuchKey':
                print('The specified key does not exist.')
              else:
                print(f"{e.response['Error']['Code']}\n{e.response['Error']['Message']}")


    @staticmethod
    # maybe change this so that the credentials pathway is passed in?
    def __retrieve_api_authorisation() -> dict:

      with open('.credentials/api_config.json', 'r') as read_file:

        header_dict = json.load(read_file)

      return header_dict

    # move this to stores?
    # method to return the number of stores to extract from API
    # @staticmethod
    # def _get_number_of_stores(endpoint, header_dict) -> int:

    #   response = requests.get(endpoint, headers=header_dict)

    #   if response.status_code == 200:
    #     num_of_stores = response.json()['number_stores']
    #     return num_of_stores

    #   else:

    #     print(response.status_code)


    # def _initialise_stores_df_loading(self, endpoint, header_dict) -> pd.DataFrame:

    #     response = requests.get(f"{endpoint}0", headers=header_dict)

    #     if response.status_code == 200:

    #         store_data = response.json()
    #         df_store_data = pd.DataFrame([store_data])
    #         df_store_data.set_index('index', inplace=True)

    #         return df_store_data

    #     else:

    #         print(response.status_code)
    #         # return something here?

    # def _add_remaining_stores_as_rows_to_df(self, df_store_data, num_of_stores, endpoint, header_dict) -> pd.DataFrame:

    #     for i in range(1, num_of_stores):

    #       response = requests.get(f"{endpoint}{i}", headers=header_dict)

    #       if response.status_code == 200:

    #         store_data = response.json()
    #         df_new_store_data = pd.DataFrame([store_data])
    #         df_store_data = pd.concat([df_store_data, df_new_store_data], ignore_index=True)

    #       else:

    #         print(response.status_code)

    #     return df_store_data

    # # method
    # def _retrieve_stores_data(self, endpoint) -> pd.DataFrame:

    #     header_dict = self.__retrieve_api_authorisation()

    #     df_store_data = self._initialise_stores_df_loading(endpoint, header_dict)

    #     num_of_stores = self._get_number_of_stores("https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores", header_dict)

    #     if num_of_stores is not None:
    #         df_store_data = self._add_remaining_stores_as_rows_to_df(df_store_data, num_of_stores, endpoint, header_dict)

    #         return df_store_data
    #     else:
    #        print("Error: Could not retrieve num_of_stores from API.")
