import pandas as pd
from pandas.tseries.offsets import MonthEnd

from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseTableConnector


class CardData(DataExtractor, DataCleaning, DatabaseTableConnector):
    def __init__(self):
        try:
          DataExtractor.__init__(self, source_type='pdf', source_location='https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
          DatabaseTableConnector.__init__(self, table_name='dim_card_details')
        except Exception:
            print("Something went wrong trying to initialise the CardData child class")

    def _clean_card_number_data(self, cd_df: pd.DataFrame) -> pd.DataFrame:
        # remove rows where column headings were transferred over as data values
        cd_df = self._remove_rows_with_specific_value_in_specified_column(cd_df, 'card_number', 'card_number')
        #mask_formatting_errors = cd_df['card_number'] == 'card_number'
        #cd_df = cd_df[~mask_formatting_errors]

        # remove NaN values
        # cd_df.dropna(subset = ['card_number'], inplace=True)  << this method was issuing a SettingWithCopyWarning
        cd_df = self._remove_rows_with_nan_values_in_specified_column(cd_df, 'card_number') # hope this works interchangeably with isnull()
        #mask_null_values_in_card_number = cd_df['card_number'].isnull()
        #cd_df = cd_df[~mask_null_values_in_card_number]

        # cast the column to a string dtype
        self._cast_columns_to_string(cd_df, ['card_number'])

        # remove all occurences of '?' in number strings
            #cd_df.loc[:, 'card_number'] = cd_df.card_number.apply(lambda x: x.replace('?', ''))
        cd_df['card_number'] = cd_df.card_number.str.replace('?', '')

        # this leaves the rows that are erroneous - mixed alphanumeric strings for every column
        # dropping rows containing strings with non-numeric characters

        cd_df = cd_df[cd_df['card_number'].str.isnumeric()]

        return cd_df

    # see if it would work to cast to datetime64[M] without using MonthEnd etc.
    @staticmethod
    def _convert_expiry_date_to_datetime(cd_df: pd.DataFrame) -> pd.DataFrame:

        cd_df['expiry_date'] = pd.to_datetime(cd_df['expiry_date'], format='%m/%y', errors='raise')

        # convert the dates to datetime
        cd_df['expiry_date'] = cd_df['expiry_date'] + MonthEnd(0)

        return cd_df

    #@staticmethod
    #def _convert_payment_confirmation_date_to_datetime(cd_df):

        #cd_df.loc[:, 'date_payment_confirmed'] = cd_df['date_payment_confirmed'].apply(parse)

        #cd_df['date_payment_confirmed'] = pd.to_datetime(cd_df['date_payment_confirmed'], format='mixed', errors='coerce')

        #return cd_df

    def clean_extracted_data(self) -> None:

        card_data_df = self.extracted_data.copy()

        # clean up card_number column, and remove NaN values
        card_data_df = self._clean_card_number_data(card_data_df)

        card_data_df = self._convert_expiry_date_to_datetime(card_data_df)

        self._cast_columns_to_datetime64(card_data_df, ['date_payment_confirmed'], 'mixed', 'coerce', parse_first=True)
        # card_data_df = self._convert_payment_confirmation_date_to_datetime(card_data_df) - remove this static method if above works...

        # convert card_provider column to category
        self._cast_columns_to_category(card_data_df, ['card_provider'])

        setattr(self, 'cleaned_data', card_data_df)









