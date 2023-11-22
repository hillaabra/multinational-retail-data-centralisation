# %%
from sqlalchemy.dialects.postgresql import UUID, VARCHAR

from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from database_utils import DatabaseTableConnector

# %%
class DateEventsData(DataExtractor, DataCleaning, DatabaseTableConnector):
    def __init__(self):
        try:
            DataExtractor.__init__(self)
            DatabaseTableConnector.__init__(self, target_table_name='dim_date_times')
            self._source_data_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
        except Exception:
            print("Something went wrong initialising the DateEventsData child class.")

    def extract_data(self):
        extracted_data_df = self._extract_data_from_json_url(self._source_data_url)
        self._extracted_data = extracted_data_df

    def clean_extracted_data(self) -> None:

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
