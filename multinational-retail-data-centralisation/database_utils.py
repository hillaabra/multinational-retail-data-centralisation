import yaml


class DatabaseConnector:
    # Method that reads the credentials in the yaml file and returns a dictionary of the credentials
    @staticmethod
    def read_db_creds():
      with open('multinational-retail-data-centralisation/db_creds.yaml', 'r') as stream: # check relative filepath later
          dict_db_creds = yaml.safe_load(stream)

      return dict_db_creds

test = DatabaseConnector()
result = test.read_db_creds()
print(result)