# %%
from sqlalchemy.dialects.postgresql import SMALLINT, UUID, VARCHAR

from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseTableConnector

# %%

class OrdersData(DataExtractor, DataCleaning, DatabaseTableConnector):
    def __init__(self):
        try:
          DataExtractor.__init__(self, source_type='AWS_RDS_resource', source_location='orders_table')
          DatabaseTableConnector.__init__(self, table_name='orders_table',\
                                           dtypes_for_upload={"date_uuid": UUID,
                                                            "user_uuid": UUID,
                                                            "card_number": VARCHAR,
                                                            "store_code": VARCHAR,
                                                            "product_code": VARCHAR,
                                                            "product_quality": SMALLINT})
        except Exception:
          print("Something went wrong initialising the OrdersData child class.")

    def clean_extracted_data(self) -> None:

        od_df = self.extracted_data.copy()

        od_df.set_index('level_0', inplace=True)

        self._drop_columns(od_df, ['first_name', 'last_name', '1'])

        self._cast_columns_to_string(od_df, ['date_uuid', 'user_uuid', 'card_number', 'store_code', 'product_code'])

        self._cast_columns_to_integer(od_df, ['index', 'product_quantity'], 'raise')

        setattr(self, 'cleaned_data', od_df)

    # redefining this method from the DatabaseTableConnector class - it applies to all other tables
    # in the schema other than orders_table
    def return_column_in_common_with_orders_table(self) -> None:
        print(f"Error: This method shouldn't be used on the orders_table.")

    # redefining this method from the DatabaseTableConnector class - it applies to all other tables
    # in the schema other than orders_table
    def set_primary_key_column(self) -> None:
        print(f"Error: This method shouldn't be used on the orders_table.")

