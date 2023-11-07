
# %%
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector

# %%
dc = DatabaseConnector()
cleaned_user_data = DataCleaning.clean_user_data()
# %%
dc.upload_to_db(cleaned_user_data, 'dim_users')
# %%
