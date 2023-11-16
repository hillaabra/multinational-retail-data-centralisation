# %%
import re

from sqlalchemy.dialects.postgresql import SMALLINT, DATE, UUID, VARCHAR

from card_data import CardData
from date_events_data import DateEventsData
from orders_data import OrdersData
from products_data import ProductsData
from stores_data import StoresData
from user_data import UserData

# %%
if __name__ == "__main__":


  card_data = CardData()
  stores_data = StoresData()
  user_data = UserData()
  products_data = ProductsData()
  orders_data = OrdersData()
  date_events_data = DateEventsData()

  dataset_instances = [card_data, stores_data, user_data, products_data, orders_data, date_events_data]

  for dataset_instance in dataset_instances:
    dataset_instance.extract_data()
    dataset_instance.clean_extracted_data()

  card_data_dtypes = {'card_number': VARCHAR, 'expiry_date': VARCHAR, 'date_payment_confirmed': DATE, 'card_provider': VARCHAR}
  card_data.upload_to_db(card_data_dtypes)
  card_data.set_varchar_type_limit_to_max_char_length_of_columns(['card_number', 'expiry_date', 'card_provider'])

  date_events_dtypes = {'month': VARCHAR, 'day': VARCHAR, 'year': VARCHAR, 'time_period': VARCHAR, 'date_uuid': UUID}
  date_events_data.upload_to_db(dtypes=date_events_dtypes)
  date_events_data.set_varchar_type_limit_to_max_char_length_of_columns(['month', 'day', 'year', 'time_period'])

  orders_data_dtypes = {"date_uuid": UUID,
              "user_uuid": UUID,
              "card_number": VARCHAR,
              "store_code": VARCHAR,
              "product_code": VARCHAR,
              "product_quality": SMALLINT
    }

  orders_data.upload_to_db(orders_data_dtypes)
  orders_data.set_varchar_type_limit_to_max_char_length_of_columns(['card_number', 'store_code', 'product_code'])

  products_data_dtypes = {'date_added': DATE, 'uuid': UUID}
  products_data.upload_to_db(dtypes=products_data_dtypes)

  query1 = "ALTER TABLE dim_products\
                ADD COLUMN weight_class VARCHAR(14);"
  products_data.update_db(query1)

  query2 = "UPDATE dim_products\
                SET weight_class = CASE\
                    WHEN weight < 2 THEN 'Light'\
                    WHEN weight < 40 THEN 'Mid_Sized'\
                    WHEN weight < 140 THEN 'Heavy'\
                    ELSE 'Truck_Required'\
                END;"
  products_data.update_db(query2)

  products_data.set_varchar_type_limit_to_max_char_length_of_columns(['EAN', 'product_code'])


  query3 = 'ALTER TABLE dim_products RENAME removed TO still_available;'
  products_data.update_db(query3)

  query4 = "ALTER TABLE dim_products ALTER Still_available TYPE bool \
                USING CASE \
                    WHEN Still_available = 'Still_avaliable' THEN TRUE \
                    ELSE FALSE END;"
  products_data.update_db(query4)

  stores_data_dtypes = {"locality": VARCHAR(255), # currently text
              "opening_date": DATE, # currently 'timestamp without timezone' as I had casted it to datetime in Pandas - should I have left as string?
              "store_type": VARCHAR(255), # currently text, varchar(255) NULLABLE requested - already nullable in current form - check it stays that way
              "continent": VARCHAR(255) # currently text
              }

  stores_data.upload_to_db(stores_data_dtypes)
  stores_data.set_varchar_type_limit_to_max_char_length_of_columns(['store_code', 'country_code'])

  user_data_dtypes = {"first_name": VARCHAR(255),
            "last_name": VARCHAR(255),
            "date_of_birth": DATE,
            "country_code": VARCHAR, # set maximum length after upload to server
            "user_uuid": UUID,
            "join_date": DATE}
  user_data.upload_to_db(user_data_dtypes)

  user_data.set_varchar_type_limit_to_max_char_length_of_columns(['country_code'])

for dataset_instance in dataset_instances:
    if re.match(r'^dim', dataset_instance.table_name):
      # print(f"this dataset, {dataset_instance.table_name}, starts with dim")
      dataset_instance.set_primary_key_column()
      primary_key_column = dataset_instance.return_column_in_common_with_orders_table()
      orders_table_query1 = f'ALTER TABLE "orders_table" ALTER COLUMN "{primary_key_column}" SET NOT NULL'
      dataset_instance.update_db(orders_table_query1)
      orders_table_query2 = f'ALTER TABLE "orders_table" ADD CONSTRAINT fk_{primary_key_column} FOREIGN KEY ("{primary_key_column}") REFERENCES {dataset_instance.table_name} ("{primary_key_column}");'
      dataset_instance.update_db(orders_table_query2)

