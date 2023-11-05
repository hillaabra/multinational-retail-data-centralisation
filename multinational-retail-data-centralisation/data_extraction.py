
# import boto3
# import fastapi
import pandas as pd
import sqlalchemy
# import uvicorn

from database_utils import DatabaseConnector


class DataExtractor:

  # Method to extract database table to a pandas DataFrame, which it returns
  # Takes an instance of the DatabaseConnector class and the table name as an argument
  @staticmethod
  def read_rds_table(instance, table_name):
    engine = instance.init_db_engine()
    engine.connect()
    pd_df = pd.read_sql_table(table_name, engine)
    return pd_df


# Use the read_rds_table method to extract the table containing user data and return a pandas DataFrame.
de = DataExtractor()
dc = DatabaseConnector()
user_data = de.read_rds_table(dc, 'legacy_users')
