# %%
# import re # needed?
import numpy as np
import pandas as pd
from  data_extraction import user_data


class DataCleaning:

    # Method to clean the user data (look for NULL values,
    # errors with dates, incorrectly typed values and rows filled with the wrong info)
    def clean_user_data():

        ud_df = user_data.copy()

        # Based on tests done on copied dataframe, I want to:

        # Set index column
        ud_df.set_index('index', inplace=True)

        # Cast the "first_name" and "last_name" values to strings
        string_value_columns = ['first_name', 'last_name', 'company', 'email_address', 'address', 'country', 'user_uuid']

        for column in string_value_columns:
            ud_df[column] = ud_df[column].astype('string')

        # Delete all rows where the "first_name" is NULL using dropna
        # ud_df.dropna(subset='first_name', inplace=True)
        # Is this still needed?

        # Delete all rows where the "first_name" is equal to "NULL" string
        mask_null_string_for_first_name = ud_df[ud_df['first_name'] == "NULL"]
        ud_df = ud_df[~mask_null_string_for_first_name]

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

        # make phone_number uniform

# %%
user_data.head()
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
ud_copy.date_of_birth.info()
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
