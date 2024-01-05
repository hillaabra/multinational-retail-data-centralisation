# %%
import re

import pandas as pd
from sqlalchemy.dialects.postgresql import DATE, UUID

from ..data_cleaning import DataCleaning
from ..data_extraction import DataExtractor
from ..database_utils import DatabaseTableConnector

# %%
class ProductsData(DataExtractor, DataCleaning, DatabaseTableConnector):
    '''
    Represents the products data dataset and the methods used to extract,
    clean, manipulate and upload it. Extends from DataExtractor, DataCleaning
    and DatabaseTableConnector classes.

    Attributes:
    ----------
    _target_table_name: str
        'dim_products'
    _source_data_s3_uri: str
        Protected; AWS S3 URI of csv object (freely accessed with AWS subscription)
    '''
    def __init__(self):
        '''
        See help(ProductsData) for an accurate signature
        '''
        try:
          DataExtractor.__init__(self)
          DatabaseTableConnector.__init__(self, target_table_name='dim_products')
          self._source_data_s3_uri = 's3://data-handling-public/products.csv'
        except Exception:
            print("Something went wrong initialising the ProductsData child class")

    # define method from abstract base class to extract data from source
    def extract_data(self) -> None:
        '''
        Method inherited from abstract base class DataExtractor. Extracts the
        source data to a Pandas DataFrame using the S3 object URI stored at
        the _source_data_s3_uri attribute and saves the Pandas DataFrame to
        the class's _extracted_data attribute.
        '''
        extracted_data_df = self._extract_from_s3(self._source_data_s3_uri)
        self._extracted_data = extracted_data_df

    # method to convert product weights
    # it should take the products dataframe as an argument and return the products dataframe
    @staticmethod
    def _convert_product_weights_to_kg_float(pd_df: pd.DataFrame) -> pd.DataFrame:
        '''
        Protected; method used internally to convert the weight values
        in the 'weight' column of the DataFrame to kilograms.

        Arguments:
        ---------
        pd_df: pd.DataFrame
            Pandas DataFrame being cleaned

        Returns:
        -------
        pd.DataFrame: Pandas DataFrame being cleaned
        '''
        def helper(weight_str: str) -> float | str:
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

    # define method from abstrat base class to clean product data
    def clean_extracted_data(self) -> None:
        '''
        Method inherited from abstract base class DataCleaning. Makes a copy of
        the Pandas dataframe stored at the _extracted_data attribute, applies
        cleaning methods to this copy of the dataframe, and assigns the dataframe
        after cleaning to the class's _cleaned_data attribute.
        '''
        pd_df = self._extracted_data.copy()

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

        self._cleaned_data = pd_df
        setattr(self, 'dtypes_for_upload', {'date_added': DATE, 'uuid': UUID})

    # method to add weight_class column
    def add_weight_class_column_to_db_table(self) -> None:
        '''
        Method that adds a 'weight_class' column to the dim_products table
        in the local PostgreSQL database, which categorises the values of the
        weight_class column to 'Light', 'Mid_Sized', 'Heavy' or 'Truck_Required'.
        '''
        query = "ALTER TABLE dim_products\
            ADD COLUMN weight_class VARCHAR(14);"

        self.update_db(query)

        query2 = "UPDATE dim_products\
                    SET weight_class = CASE\
                        WHEN weight < 2 THEN 'Light'\
                        WHEN weight < 40 THEN 'Mid_Sized'\
                        WHEN weight < 140 THEN 'Heavy'\
                        ELSE 'Truck_Required'\
                    END;"

        self.update_db(query2)