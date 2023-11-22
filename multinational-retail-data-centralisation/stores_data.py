import requests
import pandas as pd

from sqlalchemy.dialects.postgresql import DATE, VARCHAR

from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseTableConnector


class StoresData(DataExtractor, DataCleaning, DatabaseTableConnector):

    def __init__(self):
        try:
          DataExtractor.__init__(self, source_data_url='https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/')
          DatabaseTableConnector.__init__(self, table_name='dim_store_details')
        except Exception:
          print("Something went wrong initialising the StoresData child class")

    # method to return the number of stores to extract from API
    @staticmethod
    def _get_number_of_stores(endpoint, header_dict) -> int:

      response = requests.get(endpoint, headers=header_dict)

      if response.status_code == 200:
        num_of_stores = response.json()['number_stores']
        return num_of_stores

      else:

        print(response.status_code)

    # method to get extract first row of store data from API and return it in a dataframe
    def _initialise_stores_df_loading(self, endpoint, header_dict) -> pd.DataFrame:

        response = requests.get(f"{endpoint}0", headers=header_dict)

        if response.status_code == 200:

            store_data = response.json()
            df_store_data = pd.DataFrame([store_data])
            df_store_data.set_index('index', inplace=True)

            return df_store_data

        else:

            print(response.status_code)
             # return something here?

    # method to extract the remaining rows of store data and add them to the created dataframe
    def _add_remaining_stores_as_rows_to_df(self, df_store_data, num_of_stores, endpoint, header_dict) -> pd.DataFrame:

        for i in range(1, num_of_stores):

          response = requests.get(f"{endpoint}{i}", headers=header_dict)

          if response.status_code == 200:

            store_data = response.json()
            df_new_store_data = pd.DataFrame([store_data])
            df_store_data = pd.concat([df_store_data, df_new_store_data], ignore_index=True)

          else:

            print(response.status_code)

        return df_store_data

    # method to retrieve the stores data from the API endpoint
    def _retrieve_stores_data(self) -> pd.DataFrame:

        header_dict = self.__retrieve_api_authorisation()

        endpoint = self._source_data_url

        df_store_data = self._initialise_stores_df_loading(endpoint, header_dict)

        num_of_stores = self._get_number_of_stores("https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores", header_dict)

        if num_of_stores is not None:
            df_store_data = self._add_remaining_stores_as_rows_to_df(df_store_data, num_of_stores, endpoint, header_dict)

            return df_store_data
        else:
           print("Error: Could not retrieve num_of_stores from API.")

    # defining abstract method from DataExtractor base class
    # this method assigns the dataframe of extracted data to the extracted_data attribute
    def extract_data(self) -> None:
       extracted_data_df = self._retrieve_stores_data()
       self._extracted_data = extracted_data_df

    # defining abstract method from DataCleaning abstract base class
    def clean_extracted_data(self) -> None:

        sd_df = self._extracted_data.copy()

        # index column not correctly handled in download
        # from API, so dropping redundant column named 'index'
        # and dropping redundant 'lat' column that has no meaningful data (only  11 non-null values)
        self._drop_columns(sd_df, ['index', 'lat'])

        # dropping rows where store_data is 'NULL' (as a string), since all other column values also 'NULL' for these rows
        sd_df = self._remove_rows_with_specific_value_in_specified_column(sd_df, 'store_code', 'NULL')

        # deleting all rows where the country_code isn't valid, since this correlates
        # with the rows that have no meaningful values
        sd_df = self._remove_rows_where_column_values_not_in_defined_list(sd_df, 'country_code', ['GB', 'DE', 'US'])

        # replacing mistyped continent values
        self._replace_values_with_mapping_dictionary(sd_df, 'continent', {'eeEurope': 'Europe', 'eeAmerica': 'America'})

        # casting address, locality, store_code and (temporarily - for regex purposes) staff-numbers to strings
        self._cast_columns_to_string(sd_df, ['address', 'locality', 'staff_numbers', 'store_code'])

        # converting longitude, latitude columns to float types
        self._cast_columns_to_float(sd_df, ['longitude', 'latitude'], 'coerce')

        # removing typos (non-numerical) characters from staff_numbers field
        sd_df['staff_numbers'] = sd_df.staff_numbers.str.replace(r'\D', '', regex=True)

        # casting staff_numbers to integer type
        self._cast_columns_to_integer(sd_df, ['staff_numbers'], 'raise')

        # changing absent string values in Web Store record to None (values in numeric columns are set to NaN)
        sd_df.at[0, 'address'] = None
        sd_df.at[0, 'locality'] = None

        # deleting rows where the store_code value is the string 'NULL' since these rows have no meaningful data
        sd_df = self._remove_rows_with_specific_value_in_specified_column(sd_df, 'store_code', 'NULL')

        # casting the columns with a small range of concrete values to category types
        self._cast_columns_to_category(sd_df, ['store_type', 'country_code', 'continent'])

        # manually replacing opening_date values that have outlying formats (majority are presented in ISO time)
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

        # casting the opening_date column to datetime
        self._cast_columns_to_datetime64(sd_df, ['opening_date'], 'mixed', 'raise')

        setattr(self, 'cleaned_data', sd_df)
        setattr(self, 'dtypes_for_upload', {"locality": VARCHAR(255),
                                            "opening_date": DATE,
                                            "store_type": VARCHAR(255),
                                            "continent": VARCHAR(255)
                                            })

