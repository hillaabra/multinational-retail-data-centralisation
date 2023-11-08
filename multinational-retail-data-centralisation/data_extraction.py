
# import boto3
# import fastapi
# %%
import pandas as pd
import sqlalchemy
# %%
import tabula
# import uvicorn

from database_utils import DatabaseConnector

# %%
class DataExtractor:

  # Method to extract database table to a pandas DataFrame, which it returns
  # Takes an instance of the DatabaseConnector class and the table name as an argument
  @staticmethod
  def read_rds_table(instance, table_name):
    engine = instance.init_db_engine()
    engine.connect()
    pd_df = pd.read_sql_table(table_name, engine)
    engine.dispose()
    return pd_df

  # method to extract data from a pdf document. takes link as argument. extracts all pages.
  # returns dataframe of extracted data
  @staticmethod
  def retrieve_pdf_data(link):

    dfs = tabula.read_pdf(link, lattice=True, pages='all', multiple_tables=False)[0]

    return dfs

# %%
# Use the read_rds_table method to extract the table containing user data and return a pandas DataFrame.
de = DataExtractor()
dc = DatabaseConnector()
user_data = de.read_rds_table(dc, 'legacy_users')

pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
card_data = de.retrieve_pdf_data(pdf_link)

