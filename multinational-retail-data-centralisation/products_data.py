# %%
import re

import pandas as pd

from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from database_utils import DatabaseTableConnector


# %%
class ProductsData(DataExtractor, DataCleaning, DatabaseTableConnector):
    def __init__(self):
        try:
          DataExtractor.__init__(self, source_type='S3_resource', source_location='s3://data-handling-public/products.csv')
          DatabaseTableConnector.__init__(self, table_name='dim_products')
        except Exception:
            print("Something went wrong initialising the ProductsData child class")

    # method to convert product weights
    # it should take the products dataframe as an argument and return the products dataframe
    @staticmethod
    def _convert_product_weights_to_kg_float(pd_df: pd.DataFrame) -> pd.DataFrame:

        def helper(weight_str: str):
            # first catchs the strings in the format'{num1} x {num2}g'
            if re.fullmatch(r'\d+\s*x\s*\d+\.?\d*g', weight_str):
                # splits the string along the 'x' into a list of two string values
                nums = weight_str.split(' x ')
                # remove anything that is not a numeric digit or decimal place
                nums[1] = re.sub(r'[^\d\.]', '', nums[1])
                return round((float(nums[0])*float(nums[1]))/1000, 4)
            # then for values already entered as 'kg'
            elif re.search('kg', weight_str):
                weight_str = re.sub(r'[^\d\.]', '', weight_str)
                return round(float(weight_str), 4)
            # then for values entered in grams ('g') or ml (for ml, using 1:1 conversion ratio)
            elif re.search(r'g|(ml)', weight_str):
                weight_str = re.sub(r'[^\d\.]', '', weight_str)
                return round((float(weight_str)/1000), 4)
            # then for values entered in ounces ('oz')
            elif re.search('oz', weight_str):
                weight_str = re.sub(r'[^\d\.]', '', weight_str)
                return round((float(weight_str)*0.02834952), 4)
            # catching exceptions
            else:
                print(f"Error: The unit measurement of {weight_str} was not accounted for in convert_product_weight_to_kg_function")
                return weight_str

        pd_df['weight'] = pd_df['weight'].apply(helper)

        return pd_df

    # method to clean product data
    def clean_extracted_data(self) -> None:

        pd_df = self.extracted_data.copy()

        # remove rows with NaN values in 'weight' - these rows have no meaningful data
        pd_df = self._remove_rows_with_nan_values_in_specified_column(pd_df, 'weight')

        # removes rows with numeric characters in the category field
        # every column in these rows are filled with meaningless alphanumeric strings
        pd_df = self._remove_rows_where_numeric_digits_are_found_in_string_column_values(pd_df, 'category')

        # convert the weight column values to a float32 type
        # values have been rounded to 2 decimal places
        pd_df = self._convert_product_weights_to_kg_float(pd_df)

        #already cast to float, but reducing float type down to maximum bits needed
        self._cast_columns_to_float(pd_df, ['weight'], 'raise')

        # convert 'product_name', 'EAN', 'product_code' and 'uuid' columns to string type
        self._cast_columns_to_string(pd_df, ['product_name', 'EAN', 'product_code','uuid'])

        # convert product_price to a float32 type
        pd_df['product_price'] = pd_df['product_price'].apply(lambda x: re.sub(r'^£', '', x)).astype('float32')

        # category type columns
        self._cast_columns_to_category(pd_df, ['category', 'removed'])

        # date_added tp datetime
        self._cast_columns_to_datetime64(pd_df, ['date_added'], '%Y-%m-%d', 'raise', parse_first=True)

        setattr(self, 'cleaned_data', pd_df)

# %%
if __name__ == "__main__":
    products_data = ProductsData()
    products_data.extract_data()
    products_data.clean_extracted_data()
# %%

# %%
products_data.upload_to_db()
# %%

# from sqlalchemy.dialects.postgresql import DATE, NUMERIC, REAL, UUID, VARCHAR
# dtypes = {'product_name': VARCHAR(255),
#           'product_price': NUMERIC,
#           'weight': REAL,
#           'category': VARCHAR,
#           'EAN': VARCHAR,
#           'date_added': DATE,
#           'uuid': UUID,
#           'removed': VARCHAR,
#           'product_code': VARCHAR
#           }

products_data.update_db()
# %%
products_data.cleaned_data.sort_values('weight')
# %%
for column in ['category', 'EAN', 'removed', 'product_code']:
  products_data.set_varchar_integer_to_max_length_of_column(column)

# %%
query = "SELECT * FROM dim_products LIMIT 5;"
products_data.update_db(query)
# %%
