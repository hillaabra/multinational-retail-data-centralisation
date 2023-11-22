from abc import ABC, abstractmethod

import psycopg2
from sqlalchemy import create_engine, inspect, text
import yaml

# abstract parent class
class DatabaseConnector(ABC):

    # initialise the DatabaseConnector child class with the yaml file containing the database credentials
    def __init__(self, credentials_yaml):
       self.__credentials_yaml = credentials_yaml
       self.engine = self._init_db_engine()
       self.table_names_in_db = []
       self._set_db_table_names()

    # Method that reads the credentials in the yaml file (saved as a property of the class)
    # and returns a dictionary of the credentials
    def _read_db_creds(self):
      with open(self.__credentials_yaml, 'r') as stream:
          dict_db_creds = yaml.safe_load(stream)

      return dict_db_creds

    # this method will be defined in two different ways in the RDSDatabaseConnector
    # and LocalDatabaseConnector classes
    @abstractmethod
    def _init_db_engine(self):
       pass

    # method to set the table_names_in_db property to a list of the tables
    # already in the database being connected to
    def _set_db_table_names(self):
      inspector = inspect(self.engine)
      table_names = inspector.get_table_names()
      self.table_names_in_db = table_names

    # method to return a list of the table names in the database being connected to
    def list_db_table_names(self):
        return self.table_names_in_db


# child class for methods relating to connecting to and extracting data
# from the AWS RDS database used for this project
class RDSDatabaseConnector(DatabaseConnector):

    # instance of class initialised with the credentials supplied for the AWS RDS database, saved as a yaml file
    def __init__(self):
       super().__init__('.credentials/remote_db_creds.yaml')

    # Method that reads the credentials from the return of _read_db_creds,
    # initialises and returns a sqlalchemy database engine
    def _init_db_engine(self):

      dict_db_creds = self._read_db_creds()

      conf = {
        'HOST': dict_db_creds['RDS_HOST'],
        'USER': dict_db_creds['RDS_USER'],
        'PASSWORD': dict_db_creds['RDS_PASSWORD'],
        'DATABASE': dict_db_creds['RDS_DATABASE'],
        'PORT': dict_db_creds['RDS_PORT']
      }

      engine = create_engine("postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}".format(**conf))

      return engine


# child class for methods relating to the postgresql database on my local server
class LocalDatabaseConnector(DatabaseConnector):

    # instance of class initialised with the credentials for the local postgresql database, saved as a yaml file
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
        # in case of creating or deleting tables - updating table_names property of object using method
        self._set_db_table_names()


# child class for methods relating to specific datasets/tables to be inputted into
# or already in the postgresql database on my local server
class DatabaseTableConnector(LocalDatabaseConnector):

    def __init__(self, target_table_name, dtypes_for_upload = None):
       super().__init__()
       self.target_table_name = target_table_name
       self._cleaned_data = None
       self.table_in_db_at_init = self._check_if_table_in_db()
       self.dtypes_for_upload = dtypes_for_upload

    #method that checks if the table is in the db
    def _check_if_table_in_db(self) -> bool:
        is_in_db = self.target_table_name in self.table_names_in_db
        if is_in_db:
          print(f"A table of the name {self.target_table_name} has already been uploaded to your local postgres sales_data database.")
        return is_in_db

    # method that uploads the _cleaned_data dataframe to the database
    # the cleaned data is stored as the _cleaned_data property of the dataset instance (initialised as None)
    def upload_to_db(self) -> None:
        print(f"Starting upload of {self.target_table_name} to local sales_data database.")
        # if table name assigned to this dataset already in the database on initialisation
        if self.table_in_db_at_init:
            user_input = input("This table already exists. Enter Y if you wish to continue. This will override the existing table.")
            # if the user wants to override the existing table with that name in the database,
            # and the current instance of the class has stored a cleaned dataset in its property _cleaned_data,
            # un upload is attempted to replace the existing dataframe
            if user_input == 'Y' and self._cleaned_data is not None:
              try:
                self.engine.execution_options(isolation_level='AUTOCOMMIT').connect()
                #
                # TO DO: ADD CODE HERE TO DROP/CHECK FOR RELEVANT PRIMARY AND FOREIGN KEYS IN EXISTING TABLES BEFORE UPLOAD
                # USE: self.return_column_in_common_with_orders_table() to get name of primary key column
                # FYI DROP CONSTRAINT currently unsupported for PRIMARY and FOREIGN KEYS in postgresql
                #
                self._cleaned_data.to_sql(self.target_table_name, self.engine, if_exists='replace', dtype=self.dtypes_for_upload)
                self.engine.dispose()
              except Exception:
                print("User input Y and _cleaned_data property is not None, table by this name already exists in db, but an error occurred in uploading to the db and replacing the table.")
                print("User input Y and _cleaned_data property is not None, table by this name already exists in db, but an error occurred in uploading to the db and replacing the table.")
            # if the user wanted to override the existing table with that name in the database, but there is not cleaned data stored on the class instance
            elif input == 'Y':
              print("The _cleaned_data property on this instance is empty. There is no dataframe to upload.")
            # if the user inputs anything else other than Y
            else:
              print(f"Upload to db of {self.target_table_name} cancelled.")
        else: # if target_table_name not already in db at initialisation
            try:
                self.engine.execution_options(isolation_level='AUTOCOMMIT').connect()
                self._cleaned_data.to_sql(self.target_table_name, self.engine, dtype=self.dtypes_for_upload)
                self.engine.dispose()
                # update table_names_in_db_property after upload
                self._set_db_table_names()
            except Exception:
                print("A table by this name doesn't already exist, but an error occurred in uploading it to the database.")

    # method to return the maximum character length of the values in a column
    def _get_max_char_length_in_column(self, column_name: str) -> int:
        with self.engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            query = f'SELECT MAX(LENGTH("{column_name}")) FROM "{self.target_table_name}";'
            result = conn.execute(text(query)).fetchone() # this is returned as a tuple (e.g. (12, 0))
        return result[0] # index to get just the numeric value

    # method to cast columns to VARCHAR(?) where ? = the maximum character length of the values in the column
    # the columns are passed in as a list of strings
    def set_varchar_type_limit_to_max_char_length_of_columns(self, column_names: list[str]) -> None:
        for column_name in column_names:
            max_length = self._get_max_char_length_in_column(column_name)
            query = f'ALTER TABLE "{self.target_table_name}" ALTER COLUMN "{column_name}" TYPE VARCHAR({max_length});'
            with self.engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
                conn.execute(text(query))

    # method to print the data types of the columns
    def print_data_types_of_columns_in_database_table(self) -> None:
        if self._check_if_table_in_db:
            query = f"SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_precision_radix,\
                    datetime_precision, udt_name, is_nullable FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{self.target_table_name}';"
            with self.engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
                result = conn.execute(text(query))
                print(result.keys())
                for row in result:
                  print(row)
        else:
            print("Error: This table does not currently exist in the local database.")

    # method to find the column the dataset has in common with orders_table,
    # to be used as the primary key
    def return_column_in_common_with_orders_table(self) -> str | None:
        if self._check_if_table_in_db:
            query = f"SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS \
                        WHERE TABLE_NAME = '{self.target_table_name}' \
                        AND COLUMN_NAME != 'index' \
                    INTERSECT \
                      SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS \
                        WHERE TABLE_NAME = 'orders_table';"
            with self.engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
                result = conn.execute(text(query)).fetchone() # this should be returned as a tuple
            column_name = result[0] # index to just get the column value
            return column_name
        else:
           print(f"Error: There is no table with the name '{self.target_table_name}' in the database.")

    # method to set the primary key column of the table
    def set_primary_key_column(self) -> None:
        column_name = self.return_column_in_common_with_orders_table()
        if column_name is not None:
            query1 = f'ALTER TABLE "{self.target_table_name}" ALTER COLUMN "{column_name}" SET NOT NULL;'
            query2 = f'ALTER TABLE "{self.target_table_name}" ADD PRIMARY KEY ("{column_name}");'
            with self.engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
                conn.execute(text(query1))
                conn.execute(text(query2))

    # method to rename column of table in database
    def rename_column_in_db_table(self, original_column_name, target_column_name) -> None:
        query = f'ALTER TABLE {self.target_table_name} RENAME "{original_column_name}" TO "{target_column_name}";'
        self.update_db(query)

