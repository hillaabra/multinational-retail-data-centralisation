import numpy as np
from sqlalchemy.dialects.postgresql import DATE, UUID, VARCHAR

from ..data_cleaning import DataCleaning
from ..data_extraction import DataExtractor
from ..database_utils import DatabaseTableConnector, RDSDatabaseConnector


class UserData(DataExtractor, DataCleaning, DatabaseTableConnector):
    '''
    Represents the user data dataset and the methods used to extract,
    clean, manipulate and upload it. Extends from DataExtractor, DataCleaning
    and DatabaseTableConnector classes.

    Attributes:
    ----------
    _target_table_name: str
        'dim_users'
    _source_db_table_name: str
        'legacy_users', the table name as it appears in the AWS RDS database
    '''
    def __init__(self):
        '''
        See help(UserData) for accurate signature
        '''
        try:
          DataExtractor.__init__(self)
          DatabaseTableConnector.__init__(self, target_table_name='dim_users')
          self._source_db_table_name = 'legacy_users'
        except Exception:
          print("Something went wrong when initialising UserData child class")

    # define method from abstract base class to extract data
    def extract_data(self):
        '''
        Method inherited from abstract base class DataExtractor. Connects to the AWS RDS
        Database by creating an instance of the RDSDatabaseConnector, and extracts the data
        from the table by the name saved on the _source_db_table_name attribute to a Pandas
        DataFrame. Saves the DataFrame containing the extracted data to the class's
        _extracted_data attribute.
        '''
        conn = RDSDatabaseConnector()
        extracted_data_df = self._read_rds_table(conn, self._source_db_table_name)
        self._extracted_data = extracted_data_df

    # Method to clean the user data (look for NULL values,
    # errors with dates, incorrectly typed values and rows filled with the wrong info)
    def clean_extracted_data(self):
        '''
        Method inherited from abstract base class DataCleaning. Makes a copy of
        the Pandas dataframe stored at the _extracted_data attribute, applies
        cleaning methods to this copy of the dataframe, and assigns the dataframe
        after cleaning to the class's _cleaned_data attribute.
        '''
        ud_df = self._extracted_data.copy()

        # Set index column
        ud_df.set_index('index', inplace=True)

        # Cast the "first_name" and "last_name" and ... values to strings
        self._cast_columns_to_string(ud_df, ['first_name', 'last_name', 'company', 'email_address', 'address', 'phone_number', 'user_uuid'])

        # Delete all rows where the "first_name" is equal to "NULL" string
        ud_df.loc[ud_df['first_name'] == "NULL", 'first_name'] = np.nan # first replace strings with an NaN value
        ud_df.dropna(subset='first_name', inplace=True)

        # Delete all rows where the "first_name" value contains a numeric digit
        ud_df = self._remove_rows_where_numeric_digits_are_found_in_string_column_values(ud_df, 'first_name')

        # Cast the "date_of_birth" and "join_date" columns to datetime values
        self._cast_columns_to_datetime64(ud_df, ['date_of_birth', 'join_date'], 'mixed', 'coerce')

        # replace 'GGB' values for 'GB' in country_code (for "United Kingdom")
        self._replace_values_with_mapping_dictionary(ud_df, 'country_code', {'GGB': 'GB'})

        # convert country and country code into category datatypes (and make uniform) - also delete one of these columns - redundant?
        self._cast_columns_to_category(ud_df, ['country_code', 'country'])

        # replace invalid phone numbers with np.nan
        ud_df = self._replace_invalid_phone_numbers_with_nan(ud_df, 'phone_number', ['GB', 'US', 'DE'])

        # make phone_number uniform: UK numbers, German numbers, US numbers - for later if there's time

        self._cleaned_data = ud_df
        setattr(self, 'dtypes_for_upload', {"first_name": VARCHAR(255),
                                            "last_name": VARCHAR(255),
                                            "date_of_birth": DATE,
                                            "country_code": VARCHAR, # set maximum length after upload to server
                                            "user_uuid": UUID,
                                            "join_date": DATE})