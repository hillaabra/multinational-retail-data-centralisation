# %%
import re
# %%
import numpy as np
import pandas as pd
# from pandas.tseries.offsets import MonthEnd
from dateutil.parser import parse
# %%
from data_extraction import extracted_card_data, extracted_products_data, extracted_stores_data, extracted_user_data
# %%


class DataCleaning:

    # Method to clean the user data (look for NULL values,
    # errors with dates, incorrectly typed values and rows filled with the wrong info)
    @staticmethod
    def clean_user_data():

        ud_df = extracted_user_data.copy()

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
    def clean_card_number_data(cd_df):
        # remove rows where column headings were transferred over as data values
        mask_formatting_errors = cd_df['card_number'] == 'card_number'
        cd_df = cd_df[~mask_formatting_errors]

        # remove NaN values
        # cd_df.dropna(subset = ['card_number'], inplace=True)  << this method was issuing a SettingWithCopyWarning

        mask_null_values_in_card_number = cd_df['card_number'].isnull()
        cd_df = cd_df[~mask_null_values_in_card_number]

        # cast the column to a string dtype
        cd_df['card_number'] = cd_df['card_number'].astype('string')

        # remove all occurences of '?' in number strings
        cd_df.loc[:, 'card_number'] = cd_df.card_number.apply(lambda x: x.replace('?', ''))

        # this leaves the rows that are erroneous - mixed alphanumeric strings for every column
        # dropping rows containing strings with non-numeric characters
        cd_df = cd_df[cd_df["card_number"].str.isnumeric()]

        return cd_df

    @staticmethod
    def convert_expiry_date_to_datetime(cd_df):

        cd_df['expiry_date'] = pd.to_datetime(cd_df['expiry_date'], format='%m/%y', errors='raise')

        # convert the dates to datetime
        cd_df['expiry_date'] = cd_df['expiry_date'] + MonthEnd(0)

        return cd_df

    @staticmethod
    def convert_payment_confirmation_date_to_datetime(cd_df):

        cd_df.loc[:, 'date_payment_confirmed'] = cd_df['date_payment_confirmed'].apply(parse)

        cd_df['date_payment_confirmed'] = pd.to_datetime(cd_df['date_payment_confirmed'], format='mixed', errors='coerce')

        return cd_df

    def clean_card_data(self):
        # clean up card_number column, and remove NaN values
        cd_df = extracted_card_data.copy()
        cd_df = self.clean_card_number_data(cd_df)

        # convert date columns to datetime
        cd_df = self.convert_expiry_date_to_datetime(cd_df)
        cd_df = self.convert_payment_confirmation_date_to_datetime(cd_df)

        # convert card_provider column to category
        cd_df['card_provider'] = cd_df['card_provider'].astype('category')

        return cd_df

    @staticmethod
    def clean_stores_data():

        sd_df = extracted_stores_data.copy()

        # index column not correctly handled in download
        # from API, so dropping redundant column named 'index'
        sd_df.drop('index', axis=1, inplace=True)

        # dropping redundant 'lat' column that has no meaningful data (only  11 non-null values)
        sd_df.drop('lat', axis=1, inplace=True)

        # dropping columns where store_data is 'NULL' (all other column values also 'NULL')
        mask_store_code_null = sd_df['store_code'] == 'NULL'
        sd_df = sd_df[~mask_store_code_null]

        # deleting all rows where the country_code isn't valid, since this correlates
        # with the rows that have no meaningful values
        mask_valid_country_codes = np.isin(sd_df['country_code'], ['GB', 'DE', 'US'])
        sd_df = sd_df[mask_valid_country_codes]

        # replacing mistyped continent values
        continent_mapping_dict = {'eeEurope': 'Europe', 'eeAmerica': 'America'}
        sd_df['continent'].replace(continent_mapping_dict, inplace=True)

        # casting address, locality, store_code and (temporarily - for regex purposes) staff-numbers to strings
        string_value_columns = ['address', 'locality', 'staff_numbers', 'store_code']

        for column in string_value_columns:

            sd_df[column] = sd_df[column].astype('string')

        # converting longitude, latitude columns to float types
        sd_df['longitude'] = pd.to_numeric(sd_df['longitude'], downcast='float', errors='coerce')
        sd_df['latitude'] = pd.to_numeric(sd_df['latitude'], downcast='float', errors='coerce')

        # removing typos (non-numerical) characters from staff_numbers field
        sd_df['staff_numbers'] = sd_df.staff_numbers.str.replace(r'\D', '', regex=True)

        # converting staff_numbers to integer type
        sd_df['staff_numbers'] = pd.to_numeric(sd_df['staff_numbers'], downcast='integer', errors='raise') # no error should be raised at this point

        # changing absent string values in Web Store record to None (values in numeric columns are set to NaN)
        sd_df.at[0, 'address'] = None
        sd_df.at[0, 'locality'] = None

        # deleting rows where the store_code value is 'NULL' since these rows have no meaningful data
        mask_store_code_nan = sd_df['store_code'] == 'NULL'
        sd_df[mask_store_code_nan]

        # casting the columns with a small range of concrete values to category types
        category_column_types = ['store_type', 'country_code', 'continent']

        for column in category_column_types:
            sd_df[column] = sd_df[column].astype('category')


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

        sd_df['opening_date'].replace(opening_date_mapping_dict, inplace=True)

        # casting the opening_date column to datetime
        sd_df['opening_date'] = pd.to_datetime(sd_df['opening_date'], format='mixed', errors='raise') # no errors should be raised at this stage

        return sd_df

    # method to convert product weights
    # it should take the products dataframe as an argument and return the products dataframe
    @staticmethod
    def convert_product_weights(pd_df):

        def convert_weight_to_kg_float(weight_str):
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

        pd_df['weight'] = pd_df['weight'].apply(convert_weight_to_kg_float)

        return pd_df

    # method to clean product data
    def clean_products_data(self):

        pd_df = extracted_products_data.copy()

        # remove rows with NaN values in 'weight' - these rows have no meaningful data
        mask_weight_values_nan = pd_df['weight'].isna()
        pd_df = pd_df[~mask_weight_values_nan]

        # removes rows with numeric characters in the category field
        # every column in these rows are filled with meaningless alphanumeric strings
        mask_category_with_digits = pd_df['category'].str.contains(pat=r'[0-9]', regex=True)
        pd_df = pd_df[~mask_category_with_digits]

        # convert the weight column values
        pd_df = self.convert_product_weights(pd_df)


        return pd_df


# %%
data_cleaner = DataCleaning()
test_pd_df = data_cleaner.clean_products_data()
# %%
test_pd_df.info()
# %%
# mask_pd_df_with_non_alphanumeric_chars = pd_df['weight'].str.contains(pat=r'[^0-9\.kgml]', regex=True)
# pd_df[mask_pd_df_with_non_alphanumeric_chars]
# showed that there are multipacks being represented as '2 x 200g' for e.g. 'Fudge 400g'
# %%


