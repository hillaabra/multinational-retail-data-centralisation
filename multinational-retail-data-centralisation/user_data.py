# %%
from sqlalchemy.dialects.postgresql import DATE, UUID, VARCHAR

from data_cleaning import UserData
from database_utils import DatabaseConnector

# %%
user_data = UserData()
user_data.extract_data()
# %%
cleaned_user_data = user_data.clean_data()
# %%
cleaned_user_data.info()

# %%
# dictionary of required data types requested for this table
dtypes = {"first_name": VARCHAR(255),
          "last_name": VARCHAR(255),
          "date_of_birth": DATE,
          "country_code": VARCHAR, # set maximum length after upload to server
          "user_uuid": UUID,
          "join_date": DATE}
conn = DatabaseConnector()
conn.upload_to_local_db(cleaned_user_data, 'dim_users_table', dtypes)
# %%
conn.set_varchar_integer_to_max_length_of_column('dim_users_table', 'country_code')
# %%
# come back to deal with issue of the dates being in NS time.
# the instructions say that at this point they're in "text" format, so maybe I should have converted them
# to strptime and just made them uniform but kept them as strings, then converted them to dates at this point?
