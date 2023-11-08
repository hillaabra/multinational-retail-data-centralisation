from data_cleaning import DataCleaning
from database_utils import DatabaseConnector

data_cleaner = DataCleaning()
cleaned_user_data = data_cleaner.clean_user_data()

conn = DatabaseConnector()
conn.upload_to_db(cleaned_user_data, 'dim_users')

cleaned_card_data = data_cleaner.clean_card_data()
conn.upload_to_db(cleaned_card_data, 'dim_card_details')