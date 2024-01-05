import pandas as pd
from sqlalchemy.dialects.postgresql import DATE, VARCHAR

from .config import card_data_config
from ..data_cleaning import DataCleaning
from ..data_extraction import DataExtractor
from ..database_utils import DatabaseTableConnector


class CardData(DataExtractor, DataCleaning, DatabaseTableConnector):
    '''
    Represents the card data dataset and the methods used to extract,
    clean, manipulate and upload it. Extends from DataExtractor, DataCleaning
    and DatabaseTableConnector classes.

    Attributes:
    ----------
    _target_table_name: str
        'dim_card_details'
    _source_data_url: str
        Protected; URL to the dataset (publicly accessed)
    '''
    def __init__(self):
        '''
        See help(CardData) for accurate signature.
        '''
        try:
          DataExtractor.__init__(self)
          DatabaseTableConnector.__init__(self, card_data_config['target_table_name'])
          self._source_data_url = card_data_config['source_data_url']
        except Exception:
            print("Something went wrong trying to initialise the CardData child class")

    # define extract_data method from the abstract DataExtractor parent class
    # this method assigns the dataframe of extracted data to the extracted_data attribute
    def extract_data(self):
        '''
        Method inherited from abstract base class DataExtractor. Extracts the
        source data using the _source_data_url attribute and saves the Pandas
        DataFrame to the class's _extracted_data attribute.
        '''
        extracted_data_df = self._retrieve_pdf_data(self._source_data_url)
        self._extracted_data = extracted_data_df

    def _clean_card_number_data(self, cd_df: pd.DataFrame) -> pd.DataFrame:
        '''
        Protected; method used internally to clean the data in the card_number column
        of the extracted data.

        Arguments:
        ---------
        cd_df: pd.DataFrame
            The card_data dataframe.

        Returns:
        -------
        cd_df: pd.DataFrame
            The card_data dataframe after the cleaning of the card_number column.
        '''
        # remove rows where column headings were transferred over as data values
        cd_df = self._remove_rows_with_specific_value_in_specified_column(cd_df, 'card_number', 'card_number')

        # remove NaN values
        cd_df = self._remove_rows_with_nan_values_in_specified_column(cd_df, 'card_number')

        # cast the column to a string dtype
        self._cast_columns_to_string(cd_df, ['card_number'])

        # remove all occurences of '?' in number strings
        cd_df['card_number'] = cd_df.card_number.str.replace('?', '')

        # this leaves the rows that are erroneous - mixed alphanumeric strings for every column
        # dropping rows containing strings with non-numeric characters
        cd_df = cd_df[cd_df['card_number'].str.isnumeric()]

        return cd_df

    # redefining the abstract method
    def clean_extracted_data(self) -> None:
        '''
        Method inherited from abstract base class DataCleaning. Makes a copy of
        the Pandas dataframe stored at the _extracted_data attribute and assigns, applies
        cleaning methods to this copy of the dataframe, and assigns the dataframe
        after cleaning to the class's _cleaned_data attribute.
        '''

        card_data_df = self._extracted_data.copy()

        # clean up card_number column, and remove NaN values
        card_data_df = self._clean_card_number_data(card_data_df)

        self._cast_columns_to_datetime64(card_data_df, ['date_payment_confirmed'], 'mixed', 'coerce', parse_first=True)

        # convert card_provider column to category
        self._cast_columns_to_category(card_data_df, ['card_provider'])

        self._cleaned_data = card_data_df
        setattr(self, 'dtypes_for_upload', {'card_number': VARCHAR,
                                            'expiry_date': VARCHAR,
                                            'date_payment_confirmed': DATE,
                                            'card_provider': VARCHAR})
