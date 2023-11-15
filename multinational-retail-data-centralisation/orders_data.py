# %%
from sqlalchemy.dialects.postgresql import BIGINT, INTEGER, SMALLINT, UUID, VARCHAR

from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseTableConnector

# %%

class OrdersData(DataExtractor, DataCleaning, DatabaseTableConnector):
    def __init__(self):
        try:
          DataExtractor.__init__(self, source_type='AWS_RDS_resource', source_location='orders_table')
          DatabaseTableConnector.__init__(self, table_name='orders_table')
        except Exception:
          print("Something went wrong initialising the OrdersData child class")

    def clean_extracted_data(self):

        od_df = self.extracted_data.copy()

        od_df.set_index('level_0', inplace=True)

        self._drop_columns(od_df, ['first_name', 'last_name', '1'])

        self._cast_columns_to_string(od_df, ['date_uuid', 'user_uuid', 'card_number', 'store_code', 'product_code'])

        self._cast_columns_to_integer(od_df, ['index', 'product_quantity'], 'raise')

        #self['cleaned_data'] = od_df
        return od_df



# check this works after checking AWS subscription
orders_data = OrdersData()
orders_data.extract_data()

orders_data.extracted_data.describe()
# %%
# in theory the rest should work...
cleaned_orders_data = orders_data.clean_extracted_data()
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

# haven't checked this but should work:
orders_data.upload_to_db(cleaned_orders_data, dtypes)
# %%
# ditto:
for column in ['card_number', 'store_code', 'product_code']:
  orders_data.set_varchar_integer_to_max_length_of_column(column)
# %%
