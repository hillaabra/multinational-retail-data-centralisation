import yaml
from sqlalchemy import create_engine


class DatabaseConnector:
    # Method that reads the credentials in the yaml file and returns a dictionary of the credentials
    @staticmethod
    def read_db_creds():
      with open('multinational-retail-data-centralisation/db_creds.yaml', 'r') as stream: # check relative filepath later
          dict_db_creds = yaml.safe_load(stream)

      return dict_db_creds

    # Method to read the credentials from the return of read_db_creds and initialise and return an sqlalchemy database engine
    def init_db_engine(self):

      dict_db_creds = self.read_db_creds()

      DATABASE_TYPE = 'postgresql'
      DBAPI = 'psycopg2'
      HOST = dict_db_creds['RDS_HOST']
      USER = dict_db_creds['RDS_USER']
      PASSWORD = dict_db_creds['RDS_PASSWORD']
      DATABASE = dict_db_creds['RDS_DATABASE']
      PORT = dict_db_creds['RDS_PORT']

      engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

      return engine


test = DatabaseConnector()
result = test.read_db_creds()
print(result)