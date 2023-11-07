
import psycopg2
import yaml
from sqlalchemy import create_engine, inspect

class DatabaseConnector:
    # Method that reads the credentials in the yaml file and returns a dictionary of the credentials
    @staticmethod
    def read_db_creds():
      with open('db_creds.yaml', 'r') as stream: # check relative filepath later
          dict_db_creds = yaml.safe_load(stream)

      return dict_db_creds

    # Method to read the credentials from the return of read_db_creds and initialise and return an sqlalchemy database engine
    def init_db_engine(self):

      dict_db_creds = self.read_db_creds()

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
      engine = self.init_db_engine()
      inspector = inspect(engine)
      table_names = inspector.get_table_names()
      return table_names

    # method that takes in a Pandas DataFrame and table name to upload to as an argument
    def upload_to_db(self, pd_df, table_name):
      engine = self.init_db_engine()
      pd_df.to_sql(table_name, engine, if_exists='replace')


# engine = test.init_db_engine()
# inspector = inspect(engine)
# result = inspector.get_table_names()
# print(result)

if __name__ == "__main__":
  test = DatabaseConnector()
  result = test.list_db_tables()
  print(result)