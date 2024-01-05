from sqlalchemy.dialects.postgresql import SMALLINT, UUID, VARCHAR

from .config import orders_data_config
from ..data_extraction import DataExtractor
from ..data_cleaning import DataCleaning
from ..database_utils import DatabaseTableConnector, RDSDatabaseConnector


class OrdersData(DataExtractor, DataCleaning, DatabaseTableConnector):
    '''
    Represents the orders data dataset, which is the single-source-of-truth
    dataset at the centre of the database schema. Contains the methods used
    to extract, clean, manipulate and upload this dataset. Extends from
    DataExtractor, DataCleaning and DatabaseTableConnector classes.

    Attributes:
    ----------
    _target_table_name: str
        Protected; extracted from orders_data_config import from config module.
        Signifies how the data should be named in the new local database.
    _source_db_table_name: str
        Protected; extracted from orders_data_config import from config module;
        the table name of the dataset in the AWS RDS database it is being extracted
        from.
    '''
    def __init__(self) -> None:
        '''
        See help(OrdersData) for an accurate signature.
        '''
        try:
            DataExtractor.__init__(self)
            DatabaseTableConnector.__init__(self, orders_data_config['target_table_name'])
            self._source_db_table_name = orders_data_config['source_db_table_name']
        except Exception:
           print("Something went wrong initialising the OrdersData child class.")

    def extract_data(self) -> None:
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

    def clean_extracted_data(self) -> None:
        '''
        Method inherited from abstract base class DataCleaning. Makes a copy of
        the Pandas dataframe stored at the _extracted_data attribute, applies
        cleaning methods to this copy of the dataframe, and assigns the dataframe
        after cleaning to the class's _cleaned_data attribute.
        '''
        od_df = self._extracted_data.copy()

        od_df.set_index('level_0', inplace=True)

        self._drop_columns(od_df, ['first_name', 'last_name', '1'])

        self._cast_columns_to_string(od_df, ['date_uuid', 'user_uuid', 'card_number', 'store_code', 'product_code'])

        self._cast_columns_to_integer(od_df, ['index', 'product_quantity'], 'raise')

        self._cleaned_data = od_df
        setattr(self, 'dtypes_for_upload', {"date_uuid": UUID,
                                            "user_uuid": UUID,
                                            "card_number": VARCHAR,
                                            "store_code": VARCHAR,
                                            "product_code": VARCHAR,
                                            "product_quality": SMALLINT})

    # redefining this method from the DatabaseTableConnector class - it applies to all other tables
    # in the schema other than orders_table
    def return_column_in_common_with_orders_table(self) -> None:
        '''
        Method that overrides behaviour of method defined in parent class.
        Not to be used for this class.
        '''
        print(f"Error: This method shouldn't be used on the orders_table.")

    # redefining this method from the DatabaseTableConnector class - it applies to all other tables
    # in the schema other than orders_table
    def set_primary_key_column(self) -> None:
        '''
        Method that overrides behaviour of method defined in parent class.
        Not to be used for this class.
        '''
        print(f"Error: This method shouldn't be used on the orders_table.")