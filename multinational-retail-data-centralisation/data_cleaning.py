# %%
from abc import ABC, abstractmethod
import re

import numpy as np
import pandas as pd

from dateutil.parser import parse
from pandas.tseries.offsets import MonthEnd
from data_extraction import DataExtractor

# %%

class DataCleaning(ABC):

    @abstractmethod
    def clean_data(self):
        pass

    @staticmethod
    def _drop_columns(df: pd.DataFrame, columns: list[str]) -> None:
        df.drop(columns=columns, inplace=True)

    @staticmethod
    def _rename_columns(df: pd.DataFrame, mapping_dict: dict[str, str]) -> None:
        df.rename(columns=mapping_dict, inplace=True)

    @staticmethod
    def _replace_values_with_mapping_dictionary(df: pd.DataFrame, column: str, mapping_dict: dict) -> None:
        df[column].replace(mapping_dict, inplace=True)

    @staticmethod
    def _cast_columns_to_category(df: pd.DataFrame, columns: list[str]) -> None:
        for column in columns:
            df[column] = df[column].astype('category')

    @staticmethod
    def _cast_columns_to_string(df: pd.DataFrame, columns: list[str]) -> None:
        for column in columns:
            df[column] = df[column].astype('string')

    @staticmethod
    def _cast_columns_to_integer(df: pd.DataFrame, columns: list[str], errors_flag: str) -> None:
        for column in columns:
            df[column] = pd.to_numeric(df[column], downcast='integer', errors=errors_flag)

    @staticmethod
    def _cast_columns_to_float(df: pd.DataFrame, columns: list[str], errors_flag: str) -> None:
        for column in columns:
            df[column] = pd.to_numeric(df[column], downcast='float', errors=errors_flag)

    @staticmethod
    def _cast_columns_to_datetime64(df: pd.DataFrame, columns: list[str], format_flag: str, errors_flag: str, parse_first: bool = False) -> None:
        for column in columns:

            if parse_first:
                try:
                    df[column] = df[column].apply(parse) # or df.loc[:, column] = df[column].apply(parse) ?
                except:
                    print("error with apply parse in _cast_clumns_to_datetime64 method")
            try:
                df[column] = pd.to_datetime(df[column], format=format_flag, errors=errors_flag)
            except:
                print("error on to_datetime cast")

    @staticmethod
    def _remove_rows_with_nan_values_in_specified_column(df: pd.DataFrame, column: str) -> pd.DataFrame:
        mask_nan_values_in_specified_column = df[column].isna()
        df = df[~mask_nan_values_in_specified_column]
        return df

    @staticmethod
    def _remove_rows_with_specific_value_in_specified_column(df: pd.DataFrame, column: str, specified_value) -> pd.DataFrame:
        mask_value_in_specified_column = df[column] == specified_value
        df = df[~mask_value_in_specified_column]
        return df

    @staticmethod
    def _remove_rows_where_numeric_digits_are_found_in_string_column_values(df: pd.DataFrame, column: str) -> pd.DataFrame:
        mask_numeric_digits_in_column = df[column].str.contains(pat=r'[0-9]', regex=True)
        df = df[~mask_numeric_digits_in_column]
        return df

    @staticmethod
    def _remove_rows_where_column_values_not_in_defined_list(df: pd.DataFrame, column: str, list_of_valid_values: list) -> pd.DataFrame:
        # mask_valid_column_values = df[column].isin(list_of_valid_values) <-- previous method used for dates event - see if works with np method instead
        mask_valid_column_values = np.isin(df[column], list_of_valid_values)
        df = df[mask_valid_column_values]
        return df

    @staticmethod
    def _replace_invalid_phone_numbers_with_nan(df: pd.DataFrame, column_name: str, country_codes: list[str]) -> pd.DataFrame:

        for country_code in country_codes:

            if country_code == 'US':
                # This regex allows for a lot of flexibility in how the number may be inputted,
                # but it counts as invalid any number where (counting from after the country code)
                # the 1st or 4th digit is 0 or 1.
                # edit this regex to also accommodate trailing whitespace?
                valid_tel_regex = r'^((0{1,2}\s?1|\+1|1)[\.\s-]?)?\(?([2-9][0-9]{2})\)?[\.\s-]?[2-9][0-9]{2}[\.\s-]?\d{4}(?:[\.\s]*((?:#|x\.?|ext\.?|extension)\s*(\d+)))?'
            elif country_code == 'GB':
                # edit this regex to also accommodate trailing whitespace?
                valid_tel_regex = r'^(?:(?:\(?(?:0(?:0|11)\)?[\s-]?\(?|\+)44\)?[\s-]?(?:\(?0\)?[\s-]?)?)|(?:\(?0))(?:(?:\d{5}\)?[\s-]?\d{4,5})|(?:\d{4}\)?[\s-]?(?:\d{5}|\d{3}[\s-]?\d{3}))|(?:\d{3}\)?[\s-]?\d{3}[\s-]?\d{3,4})|(?:\d{2}\)?[\s-]?\d{4}[\s-]?\d{4}))(?:[\s-]?(?:x|ext\.?|\#)\d{3,4})?$'
            elif country_code == 'DE':
                valid_tel_regex = r'^\s*((((00|\+)?49)((\s)|\s?\(0\)\s?)?)?|\(?(0\s?)?)?\(?(((([2-9]\d)|1[2-9])\)?[-\s](\d\s?){5,9}\d?)|((([2-9]\d{2})|1[2-9]\d)\)?[-\s]?(\d\s?){4,8}\d)|((([2-9]\d{3})|1[2-9]\d{2})\)?[-\s](\d\s?){3,7}\d?))\s*$'
            else:
                raise ValueError("Invalid country code passed to method.")

            mask_valid_tel = df[column_name].str.match(valid_tel_regex)

            df.loc[~mask_valid_tel & (df[column_name] == country_code), column_name] = np.nan

        return df


class CardData(DataExtractor, DataCleaning):
    def __init__(self):
        super().__init__('pdf', 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')

    def _clean_card_number_data(self, cd_df: pd.DataFrame) -> pd.DataFrame:
        # remove rows where column headings were transferred over as data values
        cd_df = self._remove_rows_with_specific_value_in_specified_column(cd_df, 'card_number', 'card_number')
        #mask_formatting_errors = cd_df['card_number'] == 'card_number'
        #cd_df = cd_df[~mask_formatting_errors]

        # remove NaN values
        # cd_df.dropna(subset = ['card_number'], inplace=True)  << this method was issuing a SettingWithCopyWarning
        cd_df = self._remove_rows_with_nan_values_in_specified_column(cd_df, 'card_number') # hope this works interchangeably with isnull()
        #mask_null_values_in_card_number = cd_df['card_number'].isnull()
        #cd_df = cd_df[~mask_null_values_in_card_number]

        # cast the column to a string dtype
        self._cast_columns_to_string(cd_df, ['card_number'])

        # remove all occurences of '?' in number strings
            #cd_df.loc[:, 'card_number'] = cd_df.card_number.apply(lambda x: x.replace('?', ''))
        cd_df['card_number'] = cd_df.card_number.str.replace('?', '')

        # this leaves the rows that are erroneous - mixed alphanumeric strings for every column
        # dropping rows containing strings with non-numeric characters

        cd_df = cd_df[cd_df['card_number'].str.isnumeric()]

        return cd_df

    # see if it would work to cast to datetime64[M] without using MonthEnd etc.
    @staticmethod
    def _convert_expiry_date_to_datetime(cd_df: pd.DataFrame) -> pd.DataFrame:

        cd_df['expiry_date'] = pd.to_datetime(cd_df['expiry_date'], format='%m/%y', errors='raise')

        # convert the dates to datetime
        cd_df['expiry_date'] = cd_df['expiry_date'] + MonthEnd(0)

        return cd_df

    #@staticmethod
    #def _convert_payment_confirmation_date_to_datetime(cd_df):

        #cd_df.loc[:, 'date_payment_confirmed'] = cd_df['date_payment_confirmed'].apply(parse)

        #cd_df['date_payment_confirmed'] = pd.to_datetime(cd_df['date_payment_confirmed'], format='mixed', errors='coerce')

        #return cd_df

    def clean_data(self):

        card_data_df = self.extracted_data.copy()

        # clean up card_number column, and remove NaN values
        card_data_df = self._clean_card_number_data(card_data_df)

        card_data_df = self._convert_expiry_date_to_datetime(card_data_df)

        self._cast_columns_to_datetime64(card_data_df, ['date_payment_confirmed'], 'mixed', 'coerce', parse_first=True)
        # card_data_df = self._convert_payment_confirmation_date_to_datetime(card_data_df) - remove this static method if above works...

        # convert card_provider column to category
        self._cast_columns_to_category(card_data_df, ['card_provider'])
        #self['cleaned_data'] = card_data_df

        return card_data_df


class UserData(DataExtractor, DataCleaning):
    def __init__(self):
        super().__init__('AWS_RDS_resource', 'legacy_users')

    # Method to clean the user data (look for NULL values,
    # errors with dates, incorrectly typed values and rows filled with the wrong info)
    def clean_data(self):

        ud_df = self.extracted_data.copy()

        # Set index column
        ud_df.set_index('index', inplace=True)

        # Cast the "first_name" and "last_name" and ... values to strings
        self._cast_columns_to_string(ud_df, ['first_name', 'last_name', 'company', 'email_address', 'address', 'phone_number', 'user_uuid'])

        # Delete all rows where the "first_name" is equal to "NULL" string
        ud_df.loc[ud_df['first_name'] == "NULL", 'first_name'] = np.nan # first replace strings with an NaN value
        ud_df.dropna(subset='first_name', inplace=True)

        # Delete all rows where the "first_name" value contains a numeric digit
        ud_df = self._remove_rows_where_numeric_digits_are_found_in_string_column_values(ud_df, 'first_name')

        # Cast the "date_of_birth" and "join_date" columns to datetime values
        self._cast_columns_to_datetime64(ud_df, ['date_of_birth', 'join_date'], 'mixed', 'coerce')

        # replace 'GGB' values for 'GB' in country_code (for "United Kingdom")
        self._replace_values_with_mapping_dictionary(ud_df, 'country_code', {'GGB': 'GB'})

        # convert country and country code into category datatypes (and make uniform) - also delete one of these columns - redundant?
        self._cast_columns_to_category(ud_df, ['country_code', 'country'])

        # replace invalid phone numbers with np.nan
        ud_df = self._replace_invalid_phone_numbers_with_nan(ud_df, 'phone_number', ['GB', 'US', 'DE'])

        # make phone_number uniform: UK numbers, German numbers, US numbers - for later if there's time

        #self['cleaned_data'] = ud_df
        return ud_df


class ProductsData(DataExtractor, DataCleaning):
    def __init__(self):
        super().__init__('S3_resource', 's3://data-handling-public/products.csv')

    # method to convert product weights
    # it should take the products dataframe as an argument and return the products dataframe
    @staticmethod
    def _convert_product_weights_to_kg_float(pd_df: pd.DataFrame) -> pd.DataFrame:

        def helper(weight_str: str):
            # first catchs the strings in the format'{num1} x {num2}g'
            if re.fullmatch(r'\d+\s*x\s*\d+\.?\d*g', weight_str):
                # splits the string along the 'x' into a list of two string values
                nums = weight_str.split(' x ')
                # remove anything that is not a numeric digit or decimal place
                nums[1] = re.sub(r'[^\d\.]', '', nums[1])
                return round((float(nums[0])*float(nums[1]))/1000, 2)
            # then for values already entered as 'kg'
            elif re.search('kg', weight_str):
                weight_str = re.sub(r'[^\d\.]', '', weight_str)
                return round(float(weight_str), 2)
            # then for values entered in grams ('g') or ml (for ml, using 1:1 conversion ratio)
            elif re.search(r'g|(ml)', weight_str):
                weight_str = re.sub(r'[^\d\.]', '', weight_str)
                return round((float(weight_str)/1000), 2)
            # then for values entered in ounces ('oz')
            elif re.search('oz', weight_str):
                weight_str = re.sub(r'[^\d\.]', '', weight_str)
                return round((float(weight_str)*0.02834952), 2)
            # catching exceptions
            else:
                print(f"Error: The unit measurement of {weight_str} was not accounted for in convert_product_weight_to_kg_function")
                return weight_str

        pd_df['weight'] = pd_df['weight'].apply(helper)

        return pd_df

    # method to clean product data
    def clean_data(self):

        pd_df = self.extracted_data.copy()

        # remove rows with NaN values in 'weight' - these rows have no meaningful data
        pd_df = self._remove_rows_with_nan_values_in_specified_column(pd_df, 'weight')

        # removes rows with numeric characters in the category field
        # every column in these rows are filled with meaningless alphanumeric strings
        pd_df = self._remove_rows_where_numeric_digits_are_found_in_string_column_values(pd_df, 'category')

        # convert the weight column values to a float32 type
        # values have been rounded to 2 decimal places
        pd_df = self._convert_product_weights_to_kg_float(pd_df)

        #already cast to float, but reducing float type down to maximum bits needed
        self._cast_columns_to_float(pd_df, ['weight'], 'raise')

        # convert 'product_name', 'EAN', 'product_code' and 'uuid' columns to string type
        self._cast_columns_to_string(pd_df, ['product_name', 'EAN', 'product_code','uuid'])

        # convert product_price to a float32 type
        pd_df['product_price'] = pd_df['product_price'].apply(lambda x: re.sub(r'^£', '', x)).astype('float32')

        # category type columns
        self._cast_columns_to_category(pd_df, ['category', 'removed'])

        # date_added tp datetime
        self._cast_columns_to_datetime64(pd_df, ['date_added'], '%Y-%m-%d', 'raise', parse_first=True)

        #self['cleaned_data'] = pd_df
        return pd_df


class OrdersData(DataExtractor, DataCleaning):
    def __init__(self):
        super().__init__('AWS_RDS_resource', 'orders_table')

    def clean_data(self):

        od_df = self.extracted_data.copy()

        od_df.set_index('level_0', inplace=True)

        self._drop_columns(od_df, ['first_name', 'last_name', '1'])

        self._cast_columns_to_string(od_df, ['date_uuid', 'user_uuid', 'card_number', 'store_code', 'product_code'])

        self._cast_columns_to_integer(od_df, ['index', 'product_quantity'], 'raise')

        #self['cleaned_data'] = od_df
        return od_df


class DatesEventData(DataExtractor, DataCleaning):
    def __init__(self):
        super().__init__('JSON', 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')

    def clean_data(self):

        de_df = self.extracted_data.copy()

        de_df = self._remove_rows_where_column_values_not_in_defined_list(de_df, 'time_period', ['Evening', 'Morning', 'Midday', 'Late_Hours'])

        self._cast_columns_to_category(de_df, ['time_period'])

        self._cast_columns_to_string(de_df, ['date_uuid'])
        # consider abstracting:
        de_df['timestamp'] = (de_df[['year', 'month', 'day']].agg('-'.join, axis=1) + ' ' + de_df['timestamp']).astype('datetime64[s]')

        self._rename_columns(de_df, {'timestamp': 'datetime'})
        self._drop_columns(de_df, ['month', 'year', 'day'])

        # self['cleaned_data'] = de_df
        return de_df


class StoresData(DataExtractor, DataCleaning):
    def __init__(self):
        super().__init__('api_resource', 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/')

    def clean_data(self):

        sd_df = self.extracted_data.copy()

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

        # self['cleaned_data'] = sd_df
        return sd_df