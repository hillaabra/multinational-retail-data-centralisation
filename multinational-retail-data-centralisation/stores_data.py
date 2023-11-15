# %%
from sqlalchemy.dialects.postgresql import DATE, FLOAT, SMALLINT, VARCHAR

from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseTableConnector


# %%
class StoresData(DataExtractor, DataCleaning, DatabaseTableConnector):

    def __init__(self):
        try:
          DataExtractor.__init__(self, source_type='api_resource', source_location='https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/')
          DatabaseTableConnector.__init__(self, table_name='dim_store_details')
        except Exception:
          print("Something went wrong initialising the StoresData child class")

    def clean_extracted_data(self):

        sd_df = self.extracted_data.copy()

        # index column not correctly handled in download
        # from API, so dropping redundant column named 'index'
        # and dropping redundant 'lat' column that has no meaningful data (only  11 non-null values)
        self._drop_columns(sd_df, ['index', 'lat'])

        # dropping rows where store_data is 'NULL' (as a string), since all other column values also 'NULL' for these rows
        sd_df = self._remove_rows_with_specific_value_in_specified_column(sd_df, 'store_code', 'NULL')

        # deleting all rows where the country_code isn't valid, since this correlates
        # with the rows that have no meaningful values
        sd_df = self._remove_rows_where_column_values_not_in_defined_list(sd_df, 'country_code', ['GB', 'DE', 'US'])

        # replacing mistyped continent values
        self._replace_values_with_mapping_dictionary(sd_df, 'continent', {'eeEurope': 'Europe', 'eeAmerica': 'America'})

        # casting address, locality, store_code and (temporarily - for regex purposes) staff-numbers to strings
        self._cast_columns_to_string(sd_df, ['address', 'locality', 'staff_numbers', 'store_code'])

        # converting longitude, latitude columns to float types
        self._cast_columns_to_float(sd_df, ['longitude', 'latitude'], 'coerce')

        # removing typos (non-numerical) characters from staff_numbers field
        sd_df['staff_numbers'] = sd_df.staff_numbers.str.replace(r'\D', '', regex=True)

        # casting staff_numbers to integer type
        self._cast_columns_to_integer(sd_df, ['staff_numbers'], 'raise')

        # changing absent string values in Web Store record to None (values in numeric columns are set to NaN)
        sd_df.at[0, 'address'] = None
        sd_df.at[0, 'locality'] = None

        # deleting rows where the store_code value is the string 'NULL' since these rows have no meaningful data
        sd_df = self._remove_rows_with_specific_value_in_specified_column(sd_df, 'store_code', 'NULL')

        # casting the columns with a small range of concrete values to category types
        self._cast_columns_to_category(sd_df, ['store_type', 'country_code', 'continent'])

        # manually replacing opening_date values that have outlying formats (majority are presented in ISO time)
        opening_date_mapping_dict = {'October 2012 08': '2012-10-08',
                                     'July 2015 14': '2015-07-14',
                                     '2020 February 01': '2020-02-01',
                                     'May 2003 27': '2003-05-27',
                                     '2016 November 25': '2016-11-25',
                                     'October 2006 04': '2006-10-04',
                                     '2001 May 04': '2001-05-04',
                                     '1994 November 24': '1994-11-24',
                                     'February 2009 28': '2009-02-28',
                                     'March 2015 02': '2015-03-02'}

        self._replace_values_with_mapping_dictionary(sd_df, 'opening_date', opening_date_mapping_dict)

        # casting the opening_date column to datetime
        self._cast_columns_to_datetime64(sd_df, ['opening_date'], 'mixed', 'raise')

        # self['cleaned_data'] = sd_df
        return sd_df



# %%
stores_data = StoresData()
stores_data.extract_data()
cleaned_stores_data = stores_data.clean_extracted_data()
# I got an error code when testing this 429 - I've surpassed my rate limit - try another time.
# %%
cleaned_stores_data.info()
# %%
# instructions to change the location columns where they're null to N/A - I'd already done this
# in the data_cleaning stage
# instructions to merge one of the two latitude  columns into the other so that you have one latitude columns
# I'd already deleted one of the columns at the data cleaning stage which had no meaningful values
# %%

# In theory the rest should work like so:

dtypes = {"longitude": FLOAT, # currently "real" - this stores single-precision only (32bit) - FLOAT changes this to FLOAT which represents the double-precision datatype (64bit)
          "locality": VARCHAR(255), # currently text
          "store_code": VARCHAR, # currently text, set length to max needed after upload
          "staff_numbers": SMALLINT, # I already had it going in as smallint
          "opening_date": DATE, # currently 'timestamp without timezone' as I had casted it to datetime in Pandas - should I have left as string?
          "store_type": VARCHAR(255), # currently text, varchar(255) NULLABLE requested - already nullable in current form - check it stays that way
          "latitude": FLOAT, # currently "real"
          "country_code": VARCHAR, # currently text, set length to max needed after upload
          "continent": VARCHAR(255) # currently text
          }

stores_data.upload_to_db(cleaned_stores_data, dtypes)
# %%
for column in ['store_code', 'country_code']:
  stores_data.set_varchar_integer_to_max_length_of_column(column)
# %%
# NOTES FOR LATER:

# ditto about 'opening_date' - should I have kept this as a string?

# should I have kept my floats at single precision to save space? should I have specified an n with FLOAT(n)?

# in the instructions, only one column (store_type) was requested to be nullable - but all my columns are nullable
# is this an issue?