# %%
from abc import ABC, abstractmethod
import re

import numpy as np
import pandas as pd

from dateutil.parser import parse

# %%

class DataCleaning(ABC):

    @abstractmethod
    def clean_extracted_data(self):
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
