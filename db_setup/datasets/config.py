card_data_config = {"target_table_name": "dim_card_details",
                    "source_data_url": "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"}

date_events_data_config = {"target_table_name": "dim_date_times",
                           "source_data_url": "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"}

orders_data_config = {"target_table_name": "orders_table",
                      "source_db_table_name": "orders_table"}

products_data_config = {"target_table_name": "dim_products",
                        "source_data_s3_uri": "s3://data-handling-public/products.csv"}

stores_data_config = {"target_table_name": "dim_store_details",
                      "store_details_endpoint": "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/",
                      "num_of_stores_endpoint": "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores",
                      "api_credentials_filepath": "db_setup/.credentials/api_config.json"}

user_data_config = {"target_table_name": "dim_users",
                    "source_db_table_name": "legacy_users"}