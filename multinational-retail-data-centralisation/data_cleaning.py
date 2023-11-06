# %%
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
ud_copy.index.nunique() # checking that the index values are all unique
# %%
ud_copy.first_name = ud_copy.first_name.astype('string')
ud_copy.last_name = ud_copy.last_name.astype('string')

# %%
ud_copy.date_of_birth = pd.to_datetime(ud_copy.date_of_birth, format='mixed', errors='coerce')
ud_copy.date_of_birth.info()
# %%
ud_copy.date_of_birth.describe()
# COME BACK TO THIS - the max value is 20th Nov 2006 - is this invalid? 17 years old....

# %%
ud_copy.sort_values(by='date_of_birth', axis=0).tail(50)
# SOMETHING HAS GONE WRONG IN THE NULL COLUMNS - COME BACK TO THIS.

# %%
user_data.iloc[752]
# these are all scrambled datas - could have caught this first with checking the name fields for numbers
# %%
ud_copy.date_of_birth.unique()
# %%
ud_copy.date_of_birth.describe()
# %%
mask = pd.to_datetime(ud_copy.date_of_birth, format='%Y-%m-%d', errors='coerce').notna()
# %%
inverted_mask = np.invert(mask)
# %%
ud_copy.date_of_birth[inverted_mask].unique()
# %%
ud_copy.date_of_birth[inverted_mask].nunique()
# %%
from dateutil.parser import parse

ud_copy['date_of_birth'] = ud_copy['date_of_birth'].apply(parse)
# %%
from dateutil.parser import ParserError
# %%
ud_copy2 = ud_copy.copy()
# %%
def parse_dates():
    try:
        ud_copy2['date_of_birth'] = ud_copy2['date_of_birth'].apply(parse)
    except ParserError:
        pass



parse_dates()

# %%
ud_copy2.info()
# %%
mask2 = pd.to_datetime(ud_copy.date_of_birth, format='mixed', errors='coerce').notna()
inverted_mask2 = np.invert(mask2)
ud_copy.date_of_birth[inverted_mask2].unique()
# %%
ud_copy['date_of_birth'][inverted_mask2].nunique()
# %%
# I think this is it...
ud_copy
