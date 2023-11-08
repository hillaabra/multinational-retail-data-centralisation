# import re # is this still needed?
# %%
import numpy as np
import pandas as pd
from  data_extraction import card_data, user_data


class DataCleaning:

    # Method to clean the user data (look for NULL values,
    # errors with dates, incorrectly typed values and rows filled with the wrong info)
    @staticmethod
    def clean_user_data():

        ud_df = user_data.copy()

        # Based on tests done on copied dataframe, I want to:

        # Set index column
        ud_df.set_index('index', inplace=True)

        # Cast the "first_name" and "last_name" values to strings
        string_value_columns = ['first_name', 'last_name', 'company', 'email_address', 'address', 'phone_number', 'user_uuid']

        for column in string_value_columns:
            ud_df[column] = ud_df[column].astype('string')

        # Delete all rows where the "first_name" is equal to "NULL" string
        ud_df.loc[ud_df['first_name'] == "NULL", 'first_name'] = np.nan # first replace strings with an NaN value
        ud_df.dropna(subset='first_name', inplace=True)

        # Delete all rows where the "first_name" value contains a numeric digit
        mask_numeric_digit_in_first_name = ud_df['first_name'].str.contains(pat='[0-9]', regex=True)
        ud_df = ud_df[~mask_numeric_digit_in_first_name]

        # Convert the "date_of_birth" column to datetime values
        # (either using dateutil.parse parser, or pd.to_datetime with argument "mixed"
        date_value_columns = ['date_of_birth', 'join_date']

        for column in date_value_columns:
            ud_df[column] = pd.to_datetime(ud_df[column], format='mixed', errors='coerce') # or use dateutil.parse parser?

        # replace 'GGB' values for 'GB' in country_code (for "United Kingdom")
        ud_df['country_code'].replace({'GGB': 'GB'}, inplace=True)

        # convert country and country code into category datatypes (and make uniform) - also delete one of these columns - redundant?
        category_columns = ['country_code', 'country']

        for column in category_columns:
            ud_df[column] = ud_df[column].astype('category')

        # replace invalid UK phone numbers with np.nan
        # edit this regex to also accommodate trailing whitespace?
        uk_tel_regex = r'^(?:(?:\(?(?:0(?:0|11)\)?[\s-]?\(?|\+)44\)?[\s-]?(?:\(?0\)?[\s-]?)?)|(?:\(?0))(?:(?:\d{5}\)?[\s-]?\d{4,5})|(?:\d{4}\)?[\s-]?(?:\d{5}|\d{3}[\s-]?\d{3}))|(?:\d{3}\)?[\s-]?\d{3}[\s-]?\d{3,4})|(?:\d{2}\)?[\s-]?\d{4}[\s-]?\d{4}))(?:[\s-]?(?:x|ext\.?|\#)\d{3,4})?$'
        mask_valid_uk_tel = ud_df['phone_number'].str.match(uk_tel_regex)
        ud_df.loc[~mask_valid_uk_tel & (ud_df['country_code'] == 'GB'), 'phone_number'] = np.nan

        # replace invalid US phone numbers with np.nan
        # This regex allows for a lot of flexibility in how the number may be inputted,
        # but it counts as invalid any number where (counting from after the country code)
        # the 1st or 4th digit is 0 or 1.
        # edit this regex to also accommodate trailing whitespace?
        us_tel_regex = r'^((0{1,2}\s?1|\+1|1)[\.\s-]?)?\(?([2-9][0-9]{2})\)?[\.\s-]?[2-9][0-9]{2}[\.\s-]?\d{4}(?:[\.\s]*((?:#|x\.?|ext\.?|extension)\s*(\d+)))?'
        mask_valid_us_tel = ud_df['phone_number'].str.match(us_tel_regex)
        ud_df.loc[~mask_valid_us_tel & (ud_df['country_code'] == 'US'), 'phone_number'] = np.nan


        # replace invalid DE phone numbers with np.nan
        german_tel_regex = r'^\s*((((00|\+)?49)((\s)|\s?\(0\)\s?)?)?|\(?(0\s?)?)?\(?(((([2-9]\d)|1[2-9])\)?[-\s](\d\s?){5,9}\d?)|((([2-9]\d{2})|1[2-9]\d)\)?[-\s]?(\d\s?){4,8}\d)|((([2-9]\d{3})|1[2-9]\d{2})\)?[-\s](\d\s?){3,7}\d?))\s*$'
        mask_valid_german_tel = ud_df['phone_number'].str.match(german_tel_regex)
        ud_df.loc[~mask_valid_german_tel & (ud_df['country_code'] == 'DE'), 'phone_number'] = np.nan

        # make phone_number uniform: UK numbers, German numbers, US numbers - for later if there's time


        return ud_df

    @staticmethod
    def clean_card_date():



        pass

        # remove any erroneous values, NULL values or errors with formatting

        # remove errors wtih formatting - column headings at headers of pages after p.1 were transferred over
        mask_formatting_errors = card_data['card_number'] == 'card_number'
        card_data = card_data[~mask_formatting_errors]


# if __name__ == "__main__":
  #   cleaned_user_data = DataCleaning().clean_user_data()
# %%

cd_df = card_data

# %%
mask_formatting_errors = cd_df['card_number'] == 'card_number'
cd_df[mask_formatting_errors]
# %%
cd_df = cd_df[~mask_formatting_errors]

# %%
mask_formatting_errors = cd_df['card_number'] == 'card_number'
cd_df[mask_formatting_errors]
# %%
