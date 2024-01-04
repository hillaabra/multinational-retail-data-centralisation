from sqlalchemy.dialects.postgresql import UUID, VARCHAR

from ..data_cleaning import DataCleaning
from ..data_extraction import DataExtractor
from ..database_utils import DatabaseTableConnector


class DateEventsData(DataExtractor, DataCleaning, DatabaseTableConnector):
    '''
    Represents the date events data dataset and the methods used to extract,
    clean, manipulate and upload it. Extends from DataExtractor, DataCleaning
    and DatabaseTableConnector classes.

    Attributes:
    ----------
    _target_table_name: str
        'dim_date_times'
    _source_data_url: str
        Protected; URL to the dataset (publicly accessed)
    '''
    def __init__(self):
        '''
        See help(DateEventsData) for an accurate signature
        '''
        try:
            DataExtractor.__init__(self)
            DatabaseTableConnector.__init__(self, target_table_name='dim_date_times')
            self._source_data_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
        except Exception:
            print("Something went wrong initialising the DateEventsData child class.")

    def extract_data(self):
        '''
        Method inherited from abstract base class DataExtractor. Extracts the
        source data using the _source_data_url attribute and saves the Pandas
        DataFrame to the class's _extracted_data attribute.
        '''
        extracted_data_df = self._extract_data_from_json_url(self._source_data_url)
        self._extracted_data = extracted_data_df

    def clean_extracted_data(self) -> None:
        '''
        Method inherited from abstract base class DataCleaning. Makes a copy of
        the Pandas dataframe stored at the _extracted_data attribute and assigns, applies
        cleaning methods to this copy of the dataframe, and assigns the dataframe
        after cleaning to the class's _cleaned_data attribute.
        '''
        de_df = self._extracted_data.copy()

        de_df = self._remove_rows_where_column_values_not_in_defined_list(de_df, 'time_period', ['Evening', 'Morning', 'Midday', 'Late_Hours'])

        self._cast_columns_to_category(de_df, ['time_period'])

        self._cast_columns_to_string(de_df, ['date_uuid'])

        # combining timestamp, year, month and day columns into a 'datetime' column
        de_df['datetime'] = (de_df[['year', 'month', 'day']].agg('-'.join, axis=1) + ' ' + de_df['timestamp']).astype('datetime64[s]')

        self._cleaned_data = de_df
        setattr(self, 'dtypes_for_upload', {'month': VARCHAR,
                                            'day': VARCHAR,
                                            'year': VARCHAR,
                                            'time_period': VARCHAR,
                                            'date_uuid': UUID})
