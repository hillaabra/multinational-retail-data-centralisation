import requests
import pandas as pd

from sqlalchemy.dialects.postgresql import DATE, VARCHAR

from .config import stores_data_config
from ..data_extraction import DataExtractor
from ..data_cleaning import DataCleaning
from ..database_utils import DatabaseTableConnector


class StoresData(DataExtractor, DataCleaning, DatabaseTableConnector):
    '''
    Represents the stores data dataset and the methods used to extract,
    clean, manipulate and upload it. Extends from DataExtractor, DataCleaning
    and DatabaseTableConnector classes.

    Attributes:
    ----------
    _target_table_name: str
        Protected; 'dim_store_details'
    _store_details_endpoint: str
        Protected; the API endpoint which retrieves the details of every store the business has. The endpoint is missing
        the '{num}' at the end, where {num} relates to each numbered endpoint.
    _num_of_stores_endpoint: str
        Protected; the API endpoint which retrieves the number of stores the business has.
    '''
    def __init__(self):
        '''
        See help(StoresData) for an accurate signature
        '''
        try:
          DataExtractor.__init__(self)
          DatabaseTableConnector.__init__(self, stores_data_config['target_table_name'])
          self._store_details_endpoint = stores_data_config['store_details_endpoint']
          self._num_of_stores_endpoint = stores_data_config['num_of_stores_endpoint']
          self.__api_credentials_filepath = stores_data_config['api_credentials_filepath']
        except Exception:
          print("Something went wrong initialising the StoresData child class")

    # method to return the number of stores to extract from API
    def _get_number_of_stores(self, header_dict: dict) -> int:
      '''
      Protected; a method used internally to retrieve the number of stores
      belonging to the business from an API.

      Arguments:
      ---------
      header_dict: dict
        dictionary header containing the API authentication key

      Returns:
      -------
      int: the number of stores
      '''
      response = requests.get(self._num_of_stores_endpoint, headers=header_dict)

      if response.status_code == 200:
        num_of_stores = response.json()['number_stores']
        return num_of_stores

      else:

        print(response.status_code)

    # method to get extract first row of store data from API and return it in a dataframe
    def _initialise_stores_df_loading(self, header_dict: dict) -> pd.DataFrame:
        '''
        Protected; method that begins the loading of the stores data from the API
        into a Pandas DataFrame.

        Arguments:
        ---------
        header_dict: dict
          dictionary header containing the API authentication key

        Returns:
        -------
        pd.DataFrame: a Pandas DataFrame containing the details of the first
        store in the dataset located at "{self._store_details_endpoint}0"
        '''

        response = requests.get(f"{self._store_details_endpoint}0", headers=header_dict)

        if response.status_code == 200:

            store_data = response.json()
            df_store_data = pd.DataFrame([store_data])
            df_store_data.set_index('index', inplace=True)

            return df_store_data

        else:
            print("HTTPS response code: ", response.status_code)


    # method to extract the remaining rows of store data and add them to the created dataframe
    def _add_remaining_stores_as_rows_to_df(self,
                                            df_store_data: pd.DataFrame,
                                            num_of_stores: int,
                                            header_dict: dict) -> pd.DataFrame:
        '''
        Protected; method used internally to finish loading from the API
        the remaining store details to the initialised DataFrame
        containing the first row value of the dataset.

        Arguments:
        ---------
        df_store_data: pd.DataFrame
            Pandas DataFrame containing the first row of stores data.
        num_of_stores: int
            The number of total stores, retrieved previously from the API endpoint
        header_dict: dict
            Header dictionary containing the API authentication key

        Returns:
        -------
        pd.DataFrame: Pandas DataFrame containing the stores data
        '''
        for i in range(1, num_of_stores):

          response = requests.get(f"{self._store_details_endpoint}{i}", headers=header_dict)

          if response.status_code == 200:

            store_data = response.json()
            df_new_store_data = pd.DataFrame([store_data])
            df_store_data = pd.concat([df_store_data, df_new_store_data], ignore_index=True)

          else:

            print("HTTPS response code: ", response.status_code)

        return df_store_data

    # method to retrieve the stores data from the API endpoint
    def _retrieve_stores_data(self) -> pd.DataFrame:
        '''
        Protected; method using internally to retrive the stores data
        from the API endpoints and load them into a Pandas DataFrame.

        Returns:
        -------
        pd.DataFrame: Pandas DataFrame of the extracted stores data
        '''
        header_dict = self._retrieve_api_authorisation(self.__api_credentials_filepath)
        return header_dict # test line

        # df_store_data = self._initialise_stores_df_loading(header_dict)

        # num_of_stores = self._get_number_of_stores(header_dict)

        # if num_of_stores is not None:
        #     df_store_data = self._add_remaining_stores_as_rows_to_df(df_store_data, num_of_stores, header_dict)

        #     return df_store_data
        # else:
        #    print("Error: Could not retrieve num_of_stores from API.")

    # defining abstract method from DataExtractor base class
    # this method assigns the dataframe of extracted data to the extracted_data attribute
    def extract_data(self) -> None:
      '''
      Method inherited from abstract base class DataExtractor. Extracts the
      source data to a Pandas DataFrame and saves the DataFrame to the
      class's _extracted_data attribute.
      '''
      extracted_data_df = self._retrieve_stores_data()
      # self._extracted_data = extracted_data_df

    # defining abstract method from DataCleaning abstract base class
    def clean_extracted_data(self) -> None:
        '''
        Method inherited from abstract base class DataCleaning. Makes a copy of
        the Pandas dataframe stored at the _extracted_data attribute, applies
        cleaning methods to this copy of the dataframe, and assigns the dataframe
        after cleaning to the class's _cleaned_data attribute.
        '''
        sd_df = self._extracted_data.copy()

        # index column not correctly handled in download from API
        # and 'lat' column that has no meaningful data (there is a 'latitude' column instead)
        self._drop_columns(sd_df, ['index', 'lat'])

        # these rows have no meaningful data
        sd_df = self._remove_rows_with_specific_value_in_specified_column(sd_df, 'store_code', 'NULL')

        # these rows have no meaningful data
        sd_df = self._remove_rows_where_column_values_not_in_defined_list(sd_df, 'country_code', ['GB', 'DE', 'US'])

        self._replace_values_with_mapping_dictionary(sd_df, 'continent', {'eeEurope': 'Europe', 'eeAmerica': 'America'})

        # staff-numbers temporarily cast to strings for regex purposes
        self._cast_columns_to_string(sd_df, ['address', 'locality', 'staff_numbers', 'store_code'])

        self._cast_columns_to_float(sd_df, ['longitude', 'latitude'], 'coerce')

        # removing typos (non-numerical) characters
        sd_df['staff_numbers'] = sd_df.staff_numbers.str.replace(r'\D', '', regex=True)

        self._cast_columns_to_integer(sd_df, ['staff_numbers'], 'raise')

        # 0 is the index of the record for the Web Store
        sd_df.at[0, 'address'] = None
        sd_df.at[0, 'locality'] = None

        # these rows have no meaningful data
        sd_df = self._remove_rows_with_specific_value_in_specified_column(sd_df, 'store_code', 'NULL')

        self._cast_columns_to_category(sd_df, ['store_type', 'country_code', 'continent'])

        # these are the outlying date values (the rest are presented in ISO time)
        opening_date_mapping_dict = {'October 2012 08': '2012-10-08',
                                     'July 2015 14': '2015-07-14',
                                     '2020 February 01': '2020-02-01',
                                     'May 2003 27': '2003-05-27',
                                     '2016 November 25': '2016-11-25',
                                     'October 2006 04': '2006-10-04',
                                     '2001 May 04': '2001-05-04',
                                     '1994 November 24': '1994-11-24',
                                     'February 2009 28': '2009-02-28',
                                     'March 2015 02': '2015-03-02'}

        self._replace_values_with_mapping_dictionary(sd_df, 'opening_date', opening_date_mapping_dict)

        self._cast_columns_to_datetime64(sd_df, ['opening_date'], 'mixed', 'raise')

        self._cleaned_data = sd_df
        setattr(self, 'dtypes_for_upload', {"locality": VARCHAR(255),
                                            "opening_date": DATE,
                                            "store_type": VARCHAR(255),
                                            "continent": VARCHAR(255)
                                            })