# %%
from sqlalchemy.dialects.postgresql import DATE, FLOAT, SMALLINT, VARCHAR

from data_cleaning import StoresData
from database_utils import DatabaseConnector

# %%
stores_data = StoresData()
stores_data.extract_data()
cleaned_stores_data = stores_data.clean_data()
# %%
cleaned_stores_data.info()
# %%
# instructions to change the location columns where they're null to N/A - I'd already done this
# in the data_cleaning stage
# instructions to merge one of the two latitude  columns into the other so that you have one latitude columns
# I'd already deleted one of the columns at the data cleaning stage which had no meaningful values
# %%
dtypes = {"longitude": FLOAT, # currently "real" - this stores single-precision only (32bit) - FLOAT changes this to FLOAT which represents the double-precision datatype (64bit)
          "locality": VARCHAR(255), # currently text
          "store_code": VARCHAR, # currently text, set length to max needed after upload
          "staff_numbers": SMALLINT, # I already had it going in as smallint
          "opening_date": DATE, # currently timestamp without timezone - should I have kept this as a string?
          "store_type": VARCHAR(255), # currently text, varchar(255) NULLABLE requested - already nullable in current form - check it stays that way
          "latitude": FLOAT, # currently "real"
          "country_code": VARCHAR, # currently text, set length to max needed after upload
          "continent": VARCHAR(255) # currently text
          }
conn = DatabaseConnector()
conn.upload_to_local_db(cleaned_stores_data, 'dim_store_details', dtypes)
# %%
for column in ['store_code', 'country_code']:
  conn.set_varchar_integer_to_max_length_of_column('dim_store_details', column)
# %%
# NOTES FOR LATER:

# ditto about 'opening_date' - should I have kept this as a string?

# should I have kept my floats at single precision to save space? should I have specified an n with FLOAT(n)?

# in the instructions, only one column (store_type) was requested to be nullable - but all my columns are nullable
# is this an issue?