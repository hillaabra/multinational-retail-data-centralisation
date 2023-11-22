import json
import os.path

from abc import ABC, abstractmethod

import boto3
from botocore.exceptions import ClientError
import pandas as pd
import sqlalchemy
import tabula


class DataExtractor(ABC):

    def __init__(self, extracted_data: pd.DataFrame = None) -> None:
        self._extracted_data = extracted_data

    @abstractmethod
    def extract_data(self):
       pass

    @staticmethod
    def _retrieve_pdf_data(pdf_url) -> pd.DataFrame:

      df = tabula.read_pdf(pdf_url, lattice=True, pages='all', multiple_tables=False)[0]

      return df

    @staticmethod
    def _extract_data_from_json_url(json_url) -> pd.DataFrame:

        df = pd.read_json(json_url)

        return df

    @staticmethod
    def _read_rds_table(rds_db_connector_instance, rds_table_name) -> pd.DataFrame:
      engine = rds_db_connector_instance._init_db_engine()
      engine.connect()
      df = pd.read_sql_table(rds_table_name, engine)
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

        user_input = input(f'File with name {file_name} already exists in repo.\
                        Enter 0 to extract the data from this local file. Enter 1 to delete the local file of this name\
                        and extract the data from the original S3 URI.')

        while user_input not in ['0', '1']:
            user_input = input('That was not a valid response. Enter the digit 0 to extract the data\
                        from the file already in the repo. Enter the digit 1 to delete the existing file\
                        and extract the data from the S3 URI.')

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
    def _retrieve_api_authorisation(api_credentials_filepath) -> dict:

      with open(api_credentials_filepath, 'r') as read_file:

        header_dict = json.load(read_file)

      return header_dict

