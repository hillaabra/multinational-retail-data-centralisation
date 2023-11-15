# %%
from data_cleaning import CardData, DatesEventData, OrdersData, ProductsData, StoresData, UserData

from database_utils import LocalDatabaseConnector
# %%
if __name__ == "__main__":

  conn = LocalDatabaseConnector()

  card_data = CardData()
  card_data.extract_data()
  cleaned_card_data = card_data.clean_data()
  conn.upload_to_db(cleaned_card_data, 'dim_card_details')

  user_data = UserData()
  user_data.extract_data()
  cleaned_user_data = user_data.clean_data()
  conn.upload_to_db(cleaned_user_data, 'dim_users')

  stores_data = StoresData()
  stores_data.extract_data()
  cleaned_stores_data = stores_data.clean_data()
  conn.upload_to_db(cleaned_stores_data, 'dim_store_details')

  products_data = ProductsData()
  products_data.extract_data()
  cleaned_products_data = products_data.clean_data()
  conn.upload_to_db(cleaned_products_data, 'dim_products')

  orders_data = OrdersData()
  orders_data.extract_data()
  cleaned_orders_data = orders_data.clean_data()
  conn.upload_to_db(cleaned_orders_data, 'orders_table')

  dates_event_data = DatesEventData()
  dates_event_data.extract_data()
  cleaned_dates_event_data = dates_event_data.clean_data()
  conn.upload_to_db(cleaned_dates_event_data, 'dim_date_times')