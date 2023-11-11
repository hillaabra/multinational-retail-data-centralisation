import psycopg2
import yaml
from sqlalchemy import create_engine, inspect

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
    def upload_to_local_db(self, pd_df, table_name):
      engine = self.init_local_db_engine()
      engine.execution_options(isolation_level='AUTOCOMMIT').connect()
      pd_df.to_sql(table_name, engine, if_exists='replace')
      engine.dispose()



