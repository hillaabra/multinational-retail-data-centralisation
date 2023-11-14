import psycopg2
import yaml
from sqlalchemy import create_engine, inspect, text

class DatabaseConnector:
    # Method that reads the credentials in the yaml file and returns a dictionary of the credentials
    @staticmethod
    def read_db_creds(yaml_file):
      with open(yaml_file, 'r') as stream: # check relative filepath later
          dict_db_creds = yaml.safe_load(stream)

      return dict_db_creds

    # Method to read the credentials from the return of read_db_creds and initialise and return an sqlalchemy database engine
    def init_remote_db_engine(self):

      dict_db_creds = self.read_db_creds('.credentials/remote_db_creds.yaml')

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
      engine = self.init_remote_db_engine()
      inspector = inspect(engine)
      table_names = inspector.get_table_names()
      return table_names

    # method to connect to local pgadmin database
    def init_local_db_engine(self):

      dict_db_creds = self.read_db_creds('.credentials/local_db_creds.yaml')

      DATABASE_TYPE = dict_db_creds['DATABASE_TYPE']
      DBAPI = dict_db_creds['DBAPI']
      HOST = dict_db_creds['HOST']
      USER = dict_db_creds['USER']
      PASSWORD = dict_db_creds['PASSWORD']
      DATABASE = dict_db_creds['DATABASE']
      PORT = dict_db_creds['PORT']

      engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

      return engine

    # method that takes in a Pandas DataFrame and table name to upload to as an argument
    def upload_to_local_db(self, pd_df, table_name, dtypes):
      engine = self.init_local_db_engine()
      engine.execution_options(isolation_level='AUTOCOMMIT').connect()
      pd_df.to_sql(table_name, engine, if_exists='replace', dtype=dtypes)
      engine.dispose()

    # method that takes in a query_text
    def update_local_db(self, query_text: str):
       engine = self.init_local_db_engine()
       with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
          conn.execute(text(query_text))

    @staticmethod
    def get_max_length_of_table_column(engine, table_name: str, column_name: str):
        with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            result = conn.execute(text(f"SELECT MAX(LENGTH({column_name})) FROM {table_name};")).fetchone()
            # this is returned as a tuple (e.g. (12, 0))
        return result[0] # index to get just the numeric value

    def set_varchar_integer_to_max_length_of_column(self, table_name: str, column_name: str):
        engine = self.init_local_db_engine()
        max_length = self.get_max_length_of_table_column(engine, table_name, column_name)
        query = f"ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE VARCHAR({max_length});"
        with engine.execution_options(isolation_level='AUTOCOMMIT').connect() as conn:
            result = conn.execute(text(query))
