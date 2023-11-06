# %%
import re
import numpy as np
import pandas as pd
from  data_extraction import user_data

print(type(user_data))
# I need to figure ou
class DataCleaning:

    # Method to clean the user data (look for NULL values,
    # errors with dates, incorrectly typed values and rows filled with the wrong info)
    def clean_user_data():
        pass

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
