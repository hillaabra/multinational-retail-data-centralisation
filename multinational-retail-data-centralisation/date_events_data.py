from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from database_utils import DatabaseTableConnector


class DateEventsData(DataExtractor, DataCleaning, DatabaseTableConnector):
    def __init__(self):
        try:
            DataExtractor.__init__(self, source_type='JSON', source_location='https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
            DatabaseTableConnector.__init__(self, table_name='dim_date_times')
        except Exception:
            print("Something went wrong initialising the DateEventsData child class.")

    def clean_extracted_data(self) -> None:

        de_df = self.extracted_data.copy()

        de_df = self._remove_rows_where_column_values_not_in_defined_list(de_df, 'time_period', ['Evening', 'Morning', 'Midday', 'Late_Hours'])

        self._cast_columns_to_category(de_df, ['time_period'])

        self._cast_columns_to_string(de_df, ['date_uuid'])
        # consider abstracting:
        de_df['timestamp'] = (de_df[['year', 'month', 'day']].agg('-'.join, axis=1) + ' ' + de_df['timestamp']).astype('datetime64[s]')

        self._rename_columns(de_df, {'timestamp': 'datetime'})
        self._drop_columns(de_df, ['month', 'year', 'day'])

        setattr(self, 'cleaned_data', de_df)


