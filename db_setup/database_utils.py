from abc import ABC, abstractmethod

import psycopg2
from sqlalchemy import create_engine, inspect, text
import yaml


class DatabaseConnector(ABC):
    '''
    An abstract base class providing functionality to subclasses
    RDSDatabaseConnector and LocalDatabaseConnector.

    Parameters:
    ---------
    credentials_yaml: str
        Filepath to YAML file containing the subclass's
        database credentials.

    Attributes:
    ----------
    engine: sqlalchemy.engine.Engine
        SQLAlchemy Engine object, used to provide connection to the
        database.

    table_names_in_db: list
        List of table names existing in the database. Set by protected
        method on intialisation.
    '''

    # initialise the DatabaseConnector child class with the yaml file containing the database credentials
    def __init__(self, credentials_yaml: str) -> None:
       '''
       See help(DatabaseConnector) for accurate signature.
       '''
       self.__credentials_yaml = credentials_yaml
       self.engine = self._init_db_engine()
       self.table_names_in_db = []
       self._set_db_table_names()

    def _read_db_creds(self) -> dict:
      '''
      Protected; this method is used by the subclass to load
      the database credentials from the filepath stored
      as a private attribute of the class.

      Returns:
      -------
      dict: Dictionary containing database credentials.
      '''
      with open(self.__credentials_yaml, 'r') as stream:
          dict_db_creds = yaml.safe_load(stream)

      return dict_db_creds

    # this method will be defined in two different ways in the RDSDatabaseConnector
    # and LocalDatabaseConnector classes
    @abstractmethod
    def _init_db_engine(self):
       '''
       Protected; abstract method implemented in RDSDatabaseConnector and
       LocalDatabaseConnector child classes.
       '''
       pass

    def _set_db_table_names(self) -> None:
      '''
      Protected, this method sets the table_names_in_db attribute of the class,
      which is a list of tables already in the database being connected to.
      '''
      inspector = inspect(self.engine)
      table_names = inspector.get_table_names()
      self.table_names_in_db = table_names

    def list_db_table_names(self) -> list:
      '''
      This method returns the table_names_in_db attribute of the class,
      which is a list of tables recorded in the database being connected to.

      Returns:
      -------
      list: A list of the table names in the database being connected to.
      '''
      return self.table_names_in_db


# child class for methods relating to connecting to and extracting data
# from the AWS RDS database used for this project
class RDSDatabaseConnector(DatabaseConnector):
    '''
    A child class extending DatabaseConnector, providing connection
    to the AWS RDS database specified in the credentials saved at
    "db_setup/.credentials/remote_db_creds.yaml".
    '''
    # instance of class initialised with the filepath to the credentials YAML
    # supplied for the AWS RDS database
    def __init__(self):
       '''
       See help(RDSDatabaseConnector) for accurate signature.
       '''
       super().__init__('db_setup/.credentials/remote_db_creds.yaml')

    # Method that reads the credentials from the return of _read_db_creds,
    # initialises and returns a sqlalchemy database engine
    def _init_db_engine(self):
      '''
      Protected; method that initialises and returns a SQLAlchemy engine
      to be stored as an attribute of the class.

      Returns:
      -------
      sqlalchemy.engine.Engine: a SQLAlchemy engine object to be used
      for connecting to the AWS RDS database specified in the credentials.
      '''

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
    '''
    A child class extending DatabaseConnector, providing connection
    to the local postgreSQL database specified in the credentials
    saved at "db_setup/.credentials/local_db_creds.yaml".
    '''
    # instance of class initialised with the filepath to the credentials
    # for the local postgresql database, saved as a yaml file
    def __init__(self):
       '''
       See help(LocalDatabaseConnector) for accurate signature.
       '''
       super().__init__('db_setup/.credentials/local_db_creds.yaml')

    # method to connect to local pgadmin database
    def _init_db_engine(self):
      '''
      Protected; method that initialises and returns a SQLAlchemy engine
      to be stored as an attribute of the class.

      Returns:
      -------
      sqlalchemy.engine.Engine: a SQLAlchemy engine object to be used
      for connecting to the AWS RDS database specified in the credentials.
      '''
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

    def update_db(self, query_text: str) -> None:
        '''
        Method that executes a SQL query on the local PostgreSQL database;
        updates the table_names_in_db attribute of the class after execution
        of the query in case of created or deleted tables.

        Arguments:
        ----------
        query_text: str
            The SQL query to be performed provided as a string.
        '''
        engine = self._init_db_engine()
        with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
          conn.execute(text(query_text))
        # in case of creating or deleting tables - updating table_names property of object using method
        self._set_db_table_names()


# child class for methods relating to specific datasets/tables to be inputted into
# or already in the postgresql database on my local server
class DatabaseTableConnector(LocalDatabaseConnector):
    '''
    A child class extending LocalDatabaseConnector, to be inherited by
    each dataset-specific class. Contains methods to interact with and modify
    specific tables in the database.

    Parameters:
    ---------
    target_table_name: str
        The name of the table as it should be saved in the local database.

    Attributes:
    ----------
    target_table_name: str
        The name of the table as it should be, or is, saved in the local database.

    _cleaned_data: None
        Protected. Will be replaced with a pd.DataFrame after data cleaning.

    table_in_db_at_init: bool
        Boolean value set by _check_if_table_in_db() method. True if a table
        by the name value of target_table_name is already in the database. False
        if not.
    '''

    def __init__(self, target_table_name: str) -> None:
       '''
       See help(DatabaseTableConnector) for accurate signature.
       '''
       super().__init__()
       self.target_table_name = target_table_name
       self._cleaned_data = None
       self.table_in_db_at_init = self._check_if_table_in_db()

    #method that checks if the table is in the db
    def _check_if_table_in_db(self) -> bool:
        '''
        Protected; method that checks if the value of the class attribute target_table_name
        matches the name of a table that already exists in the database.
        Used to set the table_in_db_at_init attribute of the class.
        Prints warning method to console if it returns true.

        Returns:
        -------
        bool: True if a table matching the value of the attribute target_table_name
        is already in the local database. False if a table by that name does not already
        exist there.
        '''
        is_in_db = self.target_table_name in self.table_names_in_db
        if is_in_db:
          print(f"A table of the name {self.target_table_name} has already been uploaded to your local postgres sales_data database.")
        return is_in_db

    # method that uploads the _cleaned_data dataframe to the database
    # the cleaned data is stored as the _cleaned_data property of the dataset instance (initialised as None)
    def upload_to_db(self) -> None:
        '''
        Method that uploads the Pandas dataframe stored at the attribute
        _cleaned_data to the local database. Prints message to console when upload attempt is
        initialised and asks for user input if a table by the same name already exists so that the user
        can choose whether to override the existing table.
        '''
        print(f"Starting upload of {self.target_table_name} to local sales_data database.")
        # if table name assigned to this dataset already in the database on initialisation
        if self.table_in_db_at_init:
            user_input = input("This table already exists. Enter Y if you wish to continue. \
                               This will override the existing table.")
            # if the user wants to override the existing table with that name in the database,
            # and the current instance of the class has stored a cleaned dataset in its property _cleaned_data,
            # un upload is attempted to replace the existing dataframe
            if user_input == 'Y' and self._cleaned_data is not None:
              try:
                self.engine.execution_options(isolation_level='AUTOCOMMIT').connect()
                #
                # TODO: when DROP CONSTRAINT becomes supported for PRIMARY and FOREIGN KEYS in potgresql,
                # DROP/CHECK FOR RELEVANT PRIMARY AND FOREIGN KEYS IN EXISTING TABLES BEFORE UPLOAD
                # (using self.return_column_in_common_with_orders_table() to get name of primary key column)
                #
                self._cleaned_data.to_sql(self.target_table_name, self.engine, if_exists='replace', dtype=self.dtypes_for_upload)
                self.engine.dispose()
              except Exception:
                print("User input Y and _cleaned_data property is not None, table by this name already exists in db, \
                      but an error occurred in uploading to the db and replacing the table.")
            # if the user wanted to override the existing table with that name in the database,
            # but there is not cleaned data stored on the class instance
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
        '''
        Protected; method used internally by set_varchar_type_limit_to_max_char_length_of_columns
        method to get establish the maximum character length of the values in a specified column.

        Arguments:
        ---------
        column_name: str
            The name of the column to be checked.

        Returns:
        -------
        int: The integer number representing the character length of the longest string in the column.
        '''
        with self.engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            query = f'SELECT MAX(LENGTH("{column_name}")) FROM "{self.target_table_name}";'
            result = conn.execute(text(query)).fetchone() # this is returned as a tuple (e.g. (12, 0))
        return result[0] # index to get just the numeric value

    # method to cast columns to VARCHAR(?) where ? = the maximum character length of the values in the column
    # the columns are passed in as a list of strings
    def set_varchar_type_limit_to_max_char_length_of_columns(self, column_names: list[str]) -> None:
        '''
        Method to typecast columns in the list provided to VARCHAR(?), where `?` represents
        the maximum character length required.

        Arguments:
        ---------
        column_names: list[str]
            A list of the column names in the table to be typecasted to VARCHAR(?)
        '''
        for column_name in column_names:
            max_length = self._get_max_char_length_in_column(column_name)
            query = f'ALTER TABLE "{self.target_table_name}" ALTER COLUMN "{column_name}" TYPE VARCHAR({max_length});'
            with self.engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
                conn.execute(text(query))

    # method to print the data types of the columns
    def print_data_types_of_columns_in_database_table(self) -> None:
        '''
        Method that prints to the console the data types of the columns
        of the table in the local database matching the value of the class attribute
        target_table_name.
        '''
        if self._check_if_table_in_db():
            query = f"SELECT column_name, data_type, character_maximum_length, numeric_precision, numeric_precision_radix,\
                        datetime_precision, udt_name, is_nullable FROM INFORMATION_SCHEMA.COLUMNS \
                        WHERE TABLE_NAME = '{self.target_table_name}';"
            with self.engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
                result = conn.execute(text(query))
                print(result.keys())
                for row in result:
                  print(row)
        else:
            print("Error: This table does not currently exist in the local database.")

    # method to find the column the dataset has in common with orders_table,
    # to be used as the primary key
    def return_column_in_common_with_orders_table(self) -> str:
        '''
        Method used to find the column the dataset has in common with the central
        table of the star-based schema, orders_table. This is used to identify
        the column that should be assigned a primary key constraint in the local
        database.

        Returns:
        -------
        str: the name of the column that matches the name of a column in orders_table
        '''
        if self._check_if_table_in_db():
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
        '''
        Method used to set the primary key column of the dimension table in the local database.
        '''
        column_name = self.return_column_in_common_with_orders_table()
        if column_name is not None:
            query1 = f'ALTER TABLE "{self.target_table_name}" ALTER COLUMN "{column_name}" SET NOT NULL;'
            query2 = f'ALTER TABLE "{self.target_table_name}" ADD PRIMARY KEY ("{column_name}");'
            with self.engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
                conn.execute(text(query1))
                conn.execute(text(query2))

    # method to rename column of table in database
    def rename_column_in_db_table(self, original_column_name: str, target_column_name: str) -> None:
        '''
        Method used to rename a column of the table in the local database.

        Arguments:
        ---------
        original_column_name: str
            The name of the column in the table to be changed.
        target_column_name: str
            The name the column should be changed to.
        '''
        query = f'ALTER TABLE {self.target_table_name} RENAME "{original_column_name}" TO "{target_column_name}";'
        self.update_db(query)