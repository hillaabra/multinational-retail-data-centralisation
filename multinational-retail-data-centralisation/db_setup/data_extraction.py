import json
import os.path

from abc import ABC, abstractmethod

import boto3
from botocore.exceptions import ClientError
import pandas as pd
import sqlalchemy
import tabula


class DataExtractor(ABC):
    '''
    Abstract Base Class containing the abstract method to be
    implemented and the static methods to be used by the inheriting
    dataset-related child classes.

    Parameters:
    ----------
    extracted_data: pd.DataFrame
        default value: None, can be initialised with pre-existing
        DataFrame for testing purposes.

    Attributes:
    ----------
    _extracted_data: pd.DataFrame
        Protected; initialised as None by default, but will contain a
        Pandas DataFrame of the data extracted from its source
    '''
    def __init__(self, extracted_data: pd.DataFrame = None) -> None:
        self._extracted_data = extracted_data

    @abstractmethod
    def extract_data(self):
      '''
      Abstract method for extracting the data, to be implemented in the
      inheriting dataset-related child classes.
      '''
      pass

    @staticmethod
    def _retrieve_pdf_data(pdf_url: str) -> pd.DataFrame:
      '''
      Protected; method that uses Tabula to read and load a table from a PDF file to
      a Pandas DataFrame.

      Arguments:
      ---------
      pdf_url: str
          The URL for the PDF to be read.

      Returns:
      -------
      pd.DataFrame: a Pandas DataFrame containing the extracted dataset.
      '''

      df = tabula.read_pdf(pdf_url, lattice=True, pages='all', multiple_tables=False)[0]

      return df

    @staticmethod
    def _extract_data_from_json_url(json_url) -> pd.DataFrame:
      '''
      Protected; method that uses Pandas to read and load data from a
      JSON file to a Pandas DataFrame.

      Arguments:
      ---------
      json_url: str
          The URL for the JSON file to be read.

      Returns:
      -------
      pd.DataFrame: a Pandas DataFrame containing the extracted dataset.
      '''
      df = pd.read_json(json_url)

      return df

    @staticmethod
    def _read_rds_table(rds_db_connector_instance, rds_table_name: str) -> pd.DataFrame:
      '''
      Protected; method that reads and loads a table from the AWS RDS database
      to a Pandas DataFrame.

      Arguments:
      ---------
      rds_db_connector_instance: RDSDatabaseConnector.class.object
          An instance of the RDSDatabaseConnector class, used to
          initialise an engine to connect to the AWS RDS database.

      rds_table_name: str
          The name of the table in the AWS RDS database to be extracted.

      Returns:
      -------
      pd.DataFrame: a Pandas DataFrame containing the extracted dataset.
      '''
      engine = rds_db_connector_instance._init_db_engine()
      engine.connect()
      df = pd.read_sql_table(rds_table_name, engine)
      engine.dispose()
      return df

    @staticmethod
    def _extract_from_s3(s3_uri: str) -> pd.DataFrame:
      '''
      Protected; method that reads and loads a csv file from an AWS S3 bucket
      to a Pandas DataFrame.

      Arguments:
      ---------
      s3_uri: str
        The URI of the AWS S3 object.

      Returns:
      -------
      pd.DataFrame: a Pandas DataFrame containing the extracted dataset.
      '''
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
              # TO DO: also write code to remove csv file from project repo?
              return products_df

            except ClientError as e:

              if e.response['Error']['Code'] == 'NoSuchBucket':
                print('The specified bucket does not exist.')
              elif e.response['Error']['Code'] == 'NoSuchKey':
                print('The specified key does not exist.')
              else:
                print(f"{e.response['Error']['Code']}\n{e.response['Error']['Message']}")


    @staticmethod
    def _retrieve_api_authorisation(api_credentials_filepath: str) -> dict:
      '''
      Protected; method used internally that returns a header dictionary
      with the API authorisation credentials stored in a JSON file.

      Arguments:
      ---------
      api_credentials_filepath: str
          A filepath to to JSON file storing the API credentials.

      Returns:
      -------
      dict: Header dictionary with authorisation credentials for use in
      API request.
      '''
      with open(api_credentials_filepath, 'r') as read_file:

        header_dict = json.load(read_file)

      return header_dict

