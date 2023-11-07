# %%
import re
import numpy as np
import pandas as pd
from  data_extraction import user_data

# %%
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
        uk_subset = ud_df[ud_df['country_code'] == 'GB']
        uk_tel_regex = r'^(?:(?:\(?(?:0(?:0|11)\)?[\s-]?\(?|\+)44\)?[\s-]?(?:\(?0\)?[\s-]?)?)|(?:\(?0))(?:(?:\d{5}\)?[\s-]?\d{4,5})|(?:\d{4}\)?[\s-]?(?:\d{5}|\d{3}[\s-]?\d{3}))|(?:\d{3}\)?[\s-]?\d{3}[\s-]?\d{3,4})|(?:\d{2}\)?[\s-]?\d{4}[\s-]?\d{4}))(?:[\s-]?(?:x|ext\.?|\#)\d{3,4})?$'
        uk_subset.loc[~uk_subset['phone_number'].str.match(uk_tel_regex), 'phone_number'] = np.nan

        # replace invalid US phone numbers with np.nan
        us_subset = ud_df[ud_df['country_code'] == 'US']
        # This regex allows for a lot of flexibility in how the number may be inputted,
        # but it counts as invalid any number where (counting from after the country code)
        # the 1st or 4th digit is 0 or 1.
        # edit this regex to also accommodate trailing whitespace?
        us_tel_regex = r'^((0{1,2}\s?1|\+1|1)[\.\s-]?)?\(?([2-9][0-9]{2})\)?[\.\s-]?[2-9][0-9]{2}[\.\s-]?\d{4}(?:[\.\s]*((?:#|x\.?|ext\.?|extension)\s*(\d+)))?'
        us_subset.loc[~us_subset['phone_number'].str.match(us_tel_regex), 'phone_number'] = np.nan


        # replace invalid DE phone numbers with np.nan
        german_tel_regex = r'^\s*((((00|\+)?49)((\s)|\s?\(0\)\s?)?)?|\(?(0\s?)?)?\(?(((([2-9]\d)|1[2-9])\)?[-\s](\d\s?){5,9}\d?)|((([2-9]\d{2})|1[2-9]\d)\)?[-\s]?(\d\s?){4,8}\d)|((([2-9]\d{3})|1[2-9]\d{2})\)?[-\s](\d\s?){3,7}\d?))\s*$'
        ud_df.loc[ud_df['country_code'] == 'DE'].where(~ud_df['phone_number'].str.match(german_tel_regex), other=np.nan, inplace=True)
        # make phone_number uniform: UK numbers, German numbers, US numbers - for later if there's time

        return ud_df

# %%
test = DataCleaning()
cleaned__user_data = test.clean_user_data()
# %%
cleaned__user_data.info()
# %%
cleaned__user_data.sort_values(by='address', ascending=False)
# %%
cleaned__user_data.loc[cleaned__user_data['user_uuid'] == '4db656b2-f085-40e5-ba18-eb6372b17632']
# %%
cleaned__user_data.iloc[8827] # don't know why indexing isn't working ...
# %%
german_subset = cleaned__user_data[cleaned__user_data['country_code'] == 'DE']
german_tel_regex = r'^\s*((((00|\+)?49)((\s)|\s?\(0\)\s?)?)?|\(?(0\s?)?)?\(?(((([2-9]\d)|1[2-9])\)?[-\s](\d\s?){5,9}\d?)|((([2-9]\d{2})|1[2-9]\d)\)?[-\s]?(\d\s?){4,8}\d)|((([2-9]\d{3})|1[2-9]\d{2})\)?[-\s](\d\s?){3,7}\d?))\s*$'
# %%
cleaned__user_data.loc[cleaned__user_data['country_code'] == 'DE'][~cleaned__user_data['phone_number'].str.match(german_tel_regex), 'phone_number'] = np.nan
# %%
cleaned__user_data.loc[cleaned__user_data['user_uuid'] == '4db656b2-f085-40e5-ba18-eb6372b17632']
# %%
cleaned__user_data.loc[cleaned__user_data['country_code'] == 'DE'].where(~cleaned__user_data['phone_number'].str.match(german_tel_regex), other=np.nan, inplace=True)
# %%
ud_copy = user_data.copy()
ud_copy.set_index('index', inplace = True)
# %%
ud_copy.head()
# %%
ud_copy.info()
# %%
ud_copy.first_name = ud_copy.first_name.astype('string')
ud_copy.last_name = ud_copy.last_name.astype('string')

# %%
ud_copy.loc[ud_copy["first_name"].str.match(".*[0-9]+.*", "first_name")]
# all of these fields are bogus values throughout -safe to delete all.
# %%
# ud_copy.drop(ud_copy[ud_copy["first_name"].str.match(".*[0-9]+.*", "first_name")], inplace=True)
# this didn't work ^
# %%
# deleted rows with a digit in the first name field.
mask = ud_copy['first_name'].str.contains(pat = '[0-9]', regex=True)
ud_copy = ud_copy[~mask]
# %%

# %%
ud_copy.date_of_birth = pd.to_datetime(ud_copy.date_of_birth, format='mixed', errors='coerce')
# %%
ud_copy.join_date.info()
# %%
ud_copy.date_of_birth.describe()
# COME BACK TO THIS - the max value is 20th Nov 2006 - is this invalid? 17 years old....

# %%
ud_copy.loc[ud_copy["last_name"].str.match(".*[0-9]+.*", "last_name")]
# %%
ud_copy.loc[ud_copy['first_name'].str.contains(pat='[0-9]', regex=True)]
# %%
ud_copy.head()
# %%
ud_copy.sort_values(by='date_of_birth', axis=0).tail(50)

# %%
# Since all rows with a null first_name also has a null last_name, I'm deleting all rows with a null first_name
ud_copy.dropna(subset='first_name', inplace=True)
# THIS ISN't WORKING BECAUSE THERE ARE NULL STRINGS

# %%
ud_copy.sort_values(by='date_of_birth', axis=0).tail(50)
# %%
mask_null_strings = ud_copy['first_name'] == "NULL"
# %%
ud_copy[~mask_null_strings].sort_values(by='date_of_birth', axis=0).tail(50)
# %%
mask_null_strings_orig = user_data['first_name'] == "NULL"
user_data[mask_null_strings_orig]
# %%
## TRY THIS LATER
for col in user_data:
    user_data[col].str.contains(pat="NULL")
# %%

# %%
ud_copy.join_date = pd.to_datetime(ud_copy.join_date, format='mixed', errors='coerce')
# %%
ud_copy.sort_values(by='join_date', axis=0).head(50)
# %%
ud_copy.join_date.describe()
# %%
ud_copy['country_code'].unique()
# %%
ud_copy[['country', 'country_code']][ud_copy['country_code'] == 'GB']
# %%
ud_copy[['country', 'country_code']][ud_copy['country_code'] == 'GB']['country'].unique()
# %%
ud_copy[['country', 'country_code']][ud_copy['country_code'] == 'DE']['country'].unique()
# %%
for code in ud_copy['country_code'].unique():
    print(f"unique values for country code {code}: ", ud_copy[['country', 'country_code']][ud_copy['country_code'] == code]['country'].unique())
# %%
ud_copy['country_code'].replace({'GGB': 'GB'}, inplace=True)
# %%
ud_copy['country'].astype('category').unique()
# %%
category_columns = ['country_code', 'country']

for column in category_columns:
    ud_copy[column] = ud_copy[column].astype('category')
# %%
uk_phone_nos = ud_copy['phone_number'][ud_copy['country_code'] == 'GB']
# %%
uk_phone_nos
# %%
german_phone_nos = ud_copy['phone_number'][ud_copy['country_code'] == 'DE']
us_phone_nos = ud_copy['phone_number'][ud_copy['country_code'] == 'US']
# %%
german_phone_nos
# %%
us_phone_nos
# %%
ud_copy['phone_number'] = ud_copy['phone_number'].astype('string')
# %%
ud_copy.info()
# %%
import re
# %%

uk_tel_regex = r'^(?:(?:\(?(?:0(?:0|11)\)?[\s-]?\(?|\+)44\)?[\s-]?(?:\(?0\)?[\s-]?)?)|(?:\(?0))(?:(?:\d{5}\)?[\s-]?\d{4,5})|(?:\d{4}\)?[\s-]?(?:\d{5}|\d{3}[\s-]?\d{3}))|(?:\d{3}\)?[\s-]?\d{3}[\s-]?\d{3,4})|(?:\d{2}\)?[\s-]?\d{4}[\s-]?\d{4}))(?:[\s-]?(?:x|ext\.?|\#)\d{3,4})?$'
# Much better result ^^.
#valid_uk_nos = uk_phone_nos[uk_phone_nos.str.match(uk_tel_regex)]

# %%
# maybe this is the issue....

# valid_uk_nos = ud_copy[ud_copy['country_code'] == 'GB']['phone_number'].str.contains(pat=uk_tel_regex, regex=True)
# invalid_uk_nos = ud_copy[ud_copy['country_code'] == 'GB'][~valid_uk_nos]
# %%
uk_subset = ud_copy[ud_copy['country_code'] == 'GB']
valid_uk_nos = uk_subset['phone_number'].str.match(uk_tel_regex)

uk_subset_with_valid_uk_nos = uk_subset[valid_uk_nos]

uk_subset_with_valid_uk_nos

uk_subset_with_invalid_uk_nos = uk_subset[~valid_uk_nos]

uk_subset_with_invalid_uk_nos

# %%
uk_subset_with_invalid_uk_nos[['country_code', 'phone_number']]
uk_subset_with_valid_uk_nos[['country_code', 'phone_number']]
# %%
uk_subset.loc[~uk_subset['phone_number'].str.match(uk_tel_regex), 'phone_number'] = np.nan

# %%
uk_subset.loc[~uk_subset['phone_number'].str.match(uk_tel_regex), 'phone_number']
# %%
# this regex i made - it discounts any number where, counting from after the country  code, the 1st or 4th digits are 0 or 1.
us_tel_regex = r'^((0{1,2}\s?1|\+1|1)[\.\s-]?)?\(?([2-9][0-9]{2})\)?[\.\s-]?[2-9][0-9]{2}[\.\s-]?\d{4}(?:[\.\s]*((?:#|x\.?|ext\.?|extension)\s*(\d+)))?'
us_subset = ud_copy[ud_copy['country_code'] == 'US']

valid_us_nos = us_subset['phone_number'].str.match(us_tel_regex)

us_subset_with_valid_us_nos = us_subset[valid_us_nos]

us_subset_with_invalid_us_nos = us_subset[~valid_us_nos]

us_subset_with_invalid_us_nos[['country_code', 'phone_number']]
# %%
us_subset_with_valid_us_nos[['country_code', 'phone_number']]
# %%

# %%
## check for duplicated user_uuid
ud_copy["user_uuid"].info() # no nul entries
# %%
ud_copy["user_uuid"].nunique()
# %%
ud_copy["user_uuid"].head(50)
# %%
german_subset = ud_copy[ud_copy['country_code'] == 'DE']
# %%
mask = german_subset['phone_number'].str.contains('[A-Za-z]', regex=True)
# %%
german_subset['phone_number']
# %%
german_tel_regex = r'^\s*((((00|\+)?49)((\s)|\s?\(0\)\s?)?)?|\(?(0\s?)?)?\(?(((([2-9]\d)|1[2-9])\)?[-\s](\d\s?){5,9}\d?)|((([2-9]\d{2})|1[2-9]\d)\)?[-\s]?(\d\s?){4,8}\d)|((([2-9]\d{3})|1[2-9]\d{2})\)?[-\s](\d\s?){3,7}\d?))\s*$'
valid_german_nos = german_subset['phone_number'].str.match(german_tel_regex)

german_subset_with_valid_german_nos = german_subset[valid_german_nos]

german_subset_with_invalid_german_nos = german_subset[~valid_german_nos]

german_subset_with_invalid_german_nos[['country_code', 'phone_number']]
# %%
german_subset_with_invalid_german_nos[['country_code', 'phone_number']].head(50)
# %%

german_subset.loc[~german_subset['phone_number'].str.match(german_tel_regex), 'phone_number'] = np.nan
# %%
german_subset_with_valid_german_nos['phone_number'].head(50)
# %%

# %%
