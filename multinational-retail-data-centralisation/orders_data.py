# %%
from sqlalchemy.dialects.postgresql import BIGINT, INTEGER, SMALLINT, UUID, VARCHAR

from data_cleaning import OrdersData
from database_utils import DatabaseConnector

# %%
orders_data = OrdersData()
orders_data.extract_data()
# %%
import pandas as pd
orders_data.extracted_data.describe()

# %%
cleaned_orders_data = orders_data.clean_data()
# %%
cleaned_orders_data.describe()
# %%
dtypes = {"date_uuid": UUID,
          "user_uuid": UUID,
          "card_number": VARCHAR,
          "store_code": VARCHAR,
          "product_code": VARCHAR,
          "product_quality": SMALLINT
}
conn = DatabaseConnector()
conn.upload_to_local_db(cleaned_orders_data, 'orders_table', dtypes)
# %%
conn = DatabaseConnector()
for column in ['card_number', 'store_code', 'product_code']:
  conn.set_varchar_integer_to_max_length_of_column('orders_table', column)
# %%
