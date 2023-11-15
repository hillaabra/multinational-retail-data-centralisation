from abc import ABC, abstractmethod
import yaml

import psycopg2
from sqlalchemy import create_engine, inspect, text


class DatabaseConnector(ABC):

    # Method that reads the credentials in the yaml file and returns a dictionary of the credentials
    def __init__(self, credentials_yaml):
       self.credentials_yaml = credentials_yaml

    def _read_db_creds(self):
      with open(self.credentials_yaml, 'r') as stream: # check relative filepath later
          dict_db_creds = yaml.safe_load(stream)

      return dict_db_creds

    @abstractmethod
    def _init_db_engine(self):
       pass


class RDSDatabaseConnector(DatabaseConnector):

    def __init__(self):
       super().__init__('.credentials/remote_db_creds.yaml')

    # Method to read the credentials from the return of read_db_creds and initialise and return an sqlalchemy database engine
    def _init_db_engine(self):

      dict_db_creds = self._read_db_creds()

      conf = {
        'HOST': dict_db_creds['RDS_HOST'],
        'USER': dict_db_creds['RDS_USER'],
        'PASSWORD': dict_db_creds['RDS_PASSWORD'],
        'DATABASE': dict_db_creds['RDS_DATABASE'],
        'PORT': dict_db_creds['RDS_PORT']}

      engine = create_engine("postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}".format(**conf))

      return engine

    # method to list all the tables in the database so you know which tables you can extract data from
    def list_db_tables(self):
      engine = self._init_db_engine()
      inspector = inspect(engine)
      table_names = inspector.get_table_names()
      return table_names


class LocalDatabaseConnector(DatabaseConnector):

    def __init__(self):
       super().__init__('.credentials/local_db_creds.yaml')

    # method to connect to local pgadmin database
    def _init_db_engine(self):

      dict_db_creds = self._read_db_creds()

      DATABASE_TYPE = dict_db_creds['DATABASE_TYPE']
      DBAPI = dict_db_creds['DBAPI']
      HOST = dict_db_creds['HOST']
      USER = dict_db_creds['USER']
      PASSWORD = dict_db_creds['PASSWORD']
      DATABASE = dict_db_creds['DATABASE']
      PORT = dict_db_creds['PORT']

      engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

      return engine

    # method that takes in a query_text
    def update_db(self, query_text: str):
       engine = self._init_db_engine()
       with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
          conn.execute(text(query_text))


class DatabaseTableConnector(LocalDatabaseConnector):

    def __init__(self, table_name):
       super().__init__()
       self.table_name = table_name
       self.engine = self._init_db_engine()

     # method that takes in a Pandas DataFrame and table name to upload to as an argument
    def upload_to_db(self, pd_df, dtypes=None):
      self.engine.execution_options(isolation_level='AUTOCOMMIT').connect()
      pd_df.to_sql(self.table_name, self.engine, if_exists='replace', dtype=dtypes) # alter method so that I can call it with a dictionary too
      self.engine.dispose()

    def _get_max_length_of_table_column(self, column_name: str):
        with self.engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            query = f"SELECT MAX(LENGTH({column_name})) FROM {self.table_name};"
            result = conn.execute(text(query)).fetchone() # this is returned as a tuple (e.g. (12, 0))
        return result[0] # index to get just the numeric value

    def set_varchar_integer_to_max_length_of_column(self, column_name: str):
        max_length = self._get_max_length_of_table_column(self.table_name, column_name)
        query = f'ALTER TABLE {self.table_name} ALTER COLUMN "{column_name}" TYPE VARCHAR({max_length});'
        with self.engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            conn.execute(text(query))

    def print_data_types_of_columns_in_specified_table(self):
        query = f"SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_precision_radix, datetime_precision, udt_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{self.table_name}';"
        with self.engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            result = conn.execute(text(query))
            for row in result:
               print(row) # currently this is printing without the column headings - try with psycopg maybe?
