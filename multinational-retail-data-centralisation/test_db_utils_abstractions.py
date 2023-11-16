# %%
from database_utils import LocalDatabaseConnector, RDSDatabaseConnector
# %%
test_local = LocalDatabaseConnector()
# %%
test_local.list_db_table_names()
# %%
test_local.table_names_in_db
# %%
print(type(test_local.engine))
# %%
from card_data import CardData
card_data_test = CardData()
# %%
print(card_data_test.table_in_db_at_init)
# %%
test_RDS = RDSDatabaseConnector()
# %%
test_RDS.table_names_in_db
# %%
