import re

from card_data import CardData
from date_events_data import DateEventsData
from orders_data import OrdersData
from products_data import ProductsData
from stores_data import StoresData
from user_data import UserData


if __name__ == "__main__":

  # INITIALISING INSTANCES OF ALL DATASET CLASSES
  card_data = CardData()
  stores_data = StoresData()
  user_data = UserData()
  products_data = ProductsData()
  orders_data = OrdersData()
  date_events_data = DateEventsData()

  dataset_instances = [card_data, stores_data, user_data, products_data, orders_data, date_events_data]

  # EXTRACTING, CLEANING AND UPLOADING ALL DATA INTO LOCAL DATABASE
  for dataset_instance in dataset_instances:
    dataset_instance.extract_data()
    dataset_instance.clean_extracted_data()
    dataset_instance.upload_to_db() # see individual dataset modules for type casting specified in each upload

  # TWEAKING THE DATABASE SCHEMA IN LOCAL SERVER
      # SETTING VARCHAR CHARACTER LIMITS ON RESPECTIVE COLUMNS
  card_data.set_varchar_type_limit_to_max_char_length_of_columns(['card_number', 'expiry_date', 'card_provider'])
  date_events_data.set_varchar_type_limit_to_max_char_length_of_columns(['month', 'day', 'year', 'time_period'])
  orders_data.set_varchar_type_limit_to_max_char_length_of_columns(['card_number', 'store_code', 'product_code'])
  products_data.set_varchar_type_limit_to_max_char_length_of_columns(['EAN', 'product_code'])
  stores_data.set_varchar_type_limit_to_max_char_length_of_columns(['store_code', 'country_code'])
  user_data.set_varchar_type_limit_to_max_char_length_of_columns(['country_code'])

      # MAKING REQUIRED CHANGES TO dim_products TABLE
  products_data.add_weight_class_column_to_db_table()

  products_data.rename_column_in_db_table('removed', 'still_available')

  query = "ALTER TABLE dim_products ALTER Still_available TYPE bool \
                USING CASE \
                    WHEN Still_available = 'Still_avaliable' THEN TRUE \
                    ELSE FALSE END;"
  products_data.update_db(query)


# FINALISING THE STAR-BASED SCHEMEA: SETTING THE PRIMARY AND FOREIGN KEYS
    # if the dataset's table name starts with "dim":
    # find the column it has in common with orders_table,
    # make that column a primary key in the dim table,
    # and make the matching column in the orders_table its foreign key
for dataset_instance in dataset_instances:
    if re.match(r'^dim', dataset_instance.target_table_name):
      dataset_instance.set_primary_key_column()
      primary_key_column = dataset_instance.return_column_in_common_with_orders_table()
      orders_table_query1 = f'ALTER TABLE orders_table ALTER COLUMN "{primary_key_column}" SET NOT NULL'
      dataset_instance.update_db(orders_table_query1)
      orders_table_query2 = f'ALTER TABLE orders_table ADD CONSTRAINT fk_{primary_key_column} FOREIGN KEY ("{primary_key_column}")\
                              REFERENCES {dataset_instance.target_table_name} ("{primary_key_column}");'
      dataset_instance.update_db(orders_table_query2)
