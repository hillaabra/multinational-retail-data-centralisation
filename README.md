# Multinational Retail Data Centralisation

*Software Engineering & Data Manipulation Project - [AiCore](https://www.theaicore.com/) (November 2023)*


![Static Badge](https://img.shields.io/badge/Skills%20%26%20Knowledge-A8B78B) ![Static Badge](https://img.shields.io/badge/Software%20Design-8A2BE2) ![Static Badge](https://img.shields.io/badge/Object%20Oriented%20Programming-8A2BE2) ![Static Badge](https://img.shields.io/badge/Algorithms-8A2BE2) ![Static Badge](https://img.shields.io/badge/Data%20Structures-8A2BE2) ![Static Badge](https://img.shields.io/badge/AWS-8A2BE2) ![Static Badge](https://img.shields.io/badge/API%20Requests-8A2BE2) ![Static Badge](https://img.shields.io/badge/Error%20Handling-8A2BE2) ![Static Badge](https://img.shields.io/badge/Regular%20Expressions-8A2BE2) ![Static Badge](https://img.shields.io/badge/Command%20Line-8A2BE2) ![Static Badge](https://img.shields.io/badge/Data%20Cleaning-8A2BE2) ![Static Badge](https://img.shields.io/badge/Data%20Storage%20Optimisation-8A2BE2)  ![Static Badge](https://img.shields.io/badge/STAR--schema%20Database-8A2BE2)

![Static Badge](https://img.shields.io/badge/Languages,%20Tools%20%26%20Libraries-A8B78B) ![Static Badge](https://img.shields.io/badge/Python-8A2BE2) ![Static Badge](https://img.shields.io/badge/SQL-8A2BE2) ![Static Badge](https://img.shields.io/badge/Pandas-8A2BE2) ![Static Badge](https://img.shields.io/badge/NumPy-8A2BE2) ![Static Badge](https://img.shields.io/badge/SQLAlchemy-8A2BE2) ![Static Badge](https://img.shields.io/badge/PostgreSQL-8A2BE2) ![Static Badge](https://img.shields.io/badge/boto3-8A2BE2) ![Static Badge](https://img.shields.io/badge/AWS%20CLI-8A2BE2) ![Static Badge](https://img.shields.io/badge/pgAdmin%204-8A2BE2) ![Static Badge](https://img.shields.io/badge/Tabula-8A2BE2) ![Static Badge](https://img.shields.io/badge/Requests-8A2BE2) ![Static Badge](https://img.shields.io/badge/PyYAML-8A2BE2) ![Static Badge](https://img.shields.io/badge/JSON-8A2BE2) ![Static Badge](https://img.shields.io/badge/os.path-8A2BE2) ![Static Badge](https://img.shields.io/badge/dateutil-8A2BE2) ![Static Badge](https://img.shields.io/badge/SQLTools-8A2BE2) ![Static Badge](https://img.shields.io/badge/VS%20Code-8A2BE2) ![Static Badge](https://img.shields.io/badge/Git%20and%20GitHub-8A2BE2)

**The brief for this project was to build a centralised data solution for a multinational retail organisation, whose sales data was spread across many different data sources.**

**Once the database was in place, I conducted data analysis to extract valuable insights for the company from the newly centralised data. The results demonstrate how the new data solution gives the client the power to make informed business decisions by enabling complex cross-examination of its large datasets from one centralised location.**

## Table of Contents
* [Project Overview](#project-overview)
    * [Database Setup Package](#db-setup)
* [File Structure](#file-structure)
    * [Top-level Repository File Structure](#top-level-repo-file-structure)
    * [Database Setup Package File Structure](#db-setup-file-structure)
    * [Data Queries Directory File Structure](#data-queries-file-structure)
* [Installation](#installation)
* [Usage](#usage)
* [Findings of Data Analysis](#findings-of-data-analysis)
* [Licence](#licence)

## [Project Overview](#project-overview)

The main implementation of the project is a Python program that extracts, cleans and imports the different datasets into a PostgreSQL database. The resulting database is organised according to a star-based schema for optimised data storage and access.

The database is produced in three phases:

1.  Extracting the large datasets from multiple and varied data sources, including AWS RDS databases, AWS S3 buckets and API endpoints, and of filetypes and datatypes including PDF, CSV, JSON and PostgreSQL database tables.

2. Cleaning the extracted data using Pandas: removing rows with entirely null or erroneous values, addressing typos in string values, such as non-numeric characters in alphanumeric fields, replacing isolated invalid values with NaN or Null, type- and downcasting columns. This was to ensure consistency in the data uploaded to the local server.

3. Uploading the cleaned datasets to a local PostgreSQL database, and finalising the database schema by type- and down-casting table columns, editing and creating columns and adding key constraints. All of these measures serve to optimise the data storage and access.

The final phase of the project leverages the centralised data to extract valuable insights through data analysis:

4.  I answered a series of questions about the company's sales data using SQL. The results of these queries, which can be seen [below](#findings-of-data-analysis), provide some concrete examples of the kind of complex data analytics this new database schema makes possible.

### [Database Setup Package](#db-setup)

The program that sets up the database performs ETL **(Extract, Transform, Load)** methods on six datasets, with an additional Transform stage after loading within the PostgreSQL database to finalise the schema. This pipeline is executed by running the `db_setup` package.

I followed **OOP principles of abstraction and encapsulation** to maximise code reusability and readability, and for ease of scalablity. A dataset-specific class is defined for each of the datasets to be extracted, transformed and loaded. Each dataset-specific class inherits and implements ETL methods defined in the parent utility classes.

**In the simplified UML diagram below, DATASET is used as a stand-in to represent all the dataset-specific classes, which follow identical rules of inheritance:**

![UML Class diagram larger](readme-images/mermaid-class-diagram.png)

Each of these dataset-specific classes inherit properties and methods from:
- `DataExtractor`
- `DataCleaning`
- `DatabaseTableConnector` (the grandchild of the `DatabaseConnector` Abstract Base Class)

The methods `extract_data()` and `clean_extracted_data()` are defined abstractly in the Abstract Base Classes `DataExtractor` and `DataCleaning` before being implemented to the specifications of the dataset in its respective class using the toolkit of inherited, and some bespoke, methods.

**In the simplified UML diagram below, ETL_UTILITY_CLASSES is a stand-in used to represent all three utility classes (`DataExtractor`, `DataCleaning`, `DatabaseTableConnector`) which are multiply inherited by all dataset classes:**

![Dataset classes](readme-images/mermaid-dataset-classes.png)

Since two of the datasets were sourced from an RDS database, and querying and extracting from the RDS database relied on methods overlapping with those required for loading data into the new PostgreSQL database *(e.g. creating a SQLAlchemy engine, getting existing table names and other internal and developer functionality)*, it made sense to define an Abstract Base Class - `DatabaseConnector` - from which `LocalDatabaseConnector` and `RDSDatabaseConnector` child classes could inherit and realise functionality:
- The two child classes of DatabaseConnector provide implementations for the abstract method `_init_db_engine()`, which returns a SQLAlchemy engine for connecting to the relevant database.
- `LocalDatabaseConnector` also has developer-targetted functionality for use during testing or after setup.
- `DatabaseTableConnector` (extended by all the dataset classes) is the child class of `LocalDatabaseConnector` and defines the properties and methods by which single datasets are loaded into the new database and transfomed within it.
- The `RDSDatabaseConnector` class is **used** (rather than inherited) by the dataset-specific classes whose source data is located in the RDS database.

**In the diagram below, DATASET_NOT_FROM_RDS_DATABASE is a stand-in used to represent the other four dataset-specific classes:**

![UML class diagram](readme-images/mermaid-diagram-database-connector-classes.png)

The **Orders** and **User** datasets were sourced from the RDS Database, so the `OrdersData` and `UserData` classes use the `_init_db_engine()` method of the `RDSDatabaseConnector` class to connect to the remote RDS Database and extract the required data, i.e. in their implementation of the `extract_data()` method:

```python
def extract_data(self) -> None:
    conn = RDSDatabaseConnector()
    extracted_data_df = self._read_rds_table(conn, self._source_db_table_name)
    self._extracted_data = extracted_data_df
```

>In the ETL journey of all datasets, the extracted data is loaded into a Pandas dataframe, which is saved to an attribute of the class called `_extracted_data`. In like fashion, when the class's cleaning method is called, a copy of the dataframe saved to this attribute is cleaned, and the dataframe after cleaning is saved to a `_cleaned_data` attribute. The dataframe stored at the object's `_cleaned_data` attribute is uploaded to the new database when the dataset's `upload_to_db()` method is called.

A configuration dictionary exists for each of the dataset classes in a `config.py` file, which is accessed in the class's constructor method. Each dictionary stores key-value pairs relevant to that dataset's extraction, as well as the intended naming of the table in the newly centralised database. For exampe, the `self._source_db_table_name` property used in the `extract_data` method above, is set in the `UserData` constructor method using the value of `"source_db_table_name"` in `user_data_config`:

```python
# db_setup/datasets/config.py

user_data_config = {"target_table_name": "dim_users",
                    "source_db_table_name": "legacy_users"}
```

```python
# db_setup/datasets/user_data.py

from .config import user_data_config
from ..data_cleaning import DataCleaning
from ..data_extraction import DataExtractor
from ..database_utils import DatabaseTableConnector, RDSDatabaseConnector


class UserData(DataExtractor, DataCleaning, DatabaseTableConnector):

    def __init__(self):
        try:
            DataExtractor.__init__(self)
            DatabaseTableConnector.__init__(self, user_data_config['target_table_name'])
            self._source_db_table_name = user_data_config['source_db_table_name']
        except Exception:
            print("Something went wrong when initialising the UserData child class.")
```

This enabled a streamlined and easily scalable implementation of the ETL pipeline in the package's main script, where each dataset-specific class is imported and instantiated:

```python
# db_setup/__main__.py

# INITIALISING INSTANCES OF ALL DATASET CLASSES
card_data = CardData()
stores_data = StoresData()
user_data = UserData()
products_data = ProductsData()
orders_data = OrdersData()
date_events_data = DateEventsData()

dataset_instances = [card_data, stores_data, user_data, products_data, orders_data, date_events_data]

# EXTRACTING, CLEANING AND UPLOADING ALL DATA INTO LOCAL DATABASE
for dataset_instance in dataset_instances:
    dataset_instance.extract_data()
    dataset_instance.clean_extracted_data()
    dataset_instance.upload_to_db()
```

Since the datasets came from diverse sources and filetypes, structuring the program in this way enabled easy customisation of the final ETL methods, and also left open the possibilities for removing or adding datasets, or amending existing dataset properties and methods according to updated extraction, cleaning or loading needs.


## [File Structure](#file-structure)

### [Top-level Repository File Structure](#top-level-repo-file-structure)

![top level GitHub repo file structure](readme-images/carbon-top-level-repo-file-structure.png)

### [Database Setup Package File Structure](#db-setup-file-structure)

![db_setup directory file structure](readme-images/carbon-db-setup-file-structure.png)

For more details on running the database setup on your local machine, see [Installation](#installation) below.

### [Data Queries Directory File Structure](#data-queries-file-structure)

![querying-the-data directory file structure](readme-images/carbon-querying-the-data-file-structure.png)


## [Installation](#installation)
Please note you will not be able to set up this database if you are not a member of AiCore, since three of the datasets are private resources and require access to private authentication credentials.

If you have access to those credentials, follow these instructions to recreate the database on your local machine:

1. Recreate the miniconda environment required for the program to run using the `env.yaml` file provided:
```
$ conda env create -f env.yaml -n multinational-retail-data-centralisation
```
2. To set-up the database on your local machine, you'll first need to initialise a new postgresql database locally. I chose to use pgAdmin 4 as the graphical database management system for this.

Open your terminal in a directory of your choosing and clone this repository:
```
$ git clone https://github.com/hillaabra/multinational-retail-data-centralisation.git
```
Get inside the `db_setup` directory:
```
$ cd multinational-retail-data-centralisation/db_setup
```
Create a `.credentials` folder in this directory to hold the credentials information.
```
$ mkdir .credentials
```
3. Using your preferred text editor, add a YAML file to this `.credentials` directory called `local_db_creds.yaml`. This will contain the credentials for the new database. The file should contain the following:
```
DATABASE_TYPE: 'postgresql'
DBAPI: 'psycopg2'
HOST: 'localhost'
USER: 'postgres'
PASSWORD: # insert password
DATABASE: # insert name of database e.g. 'sales_data'
PORT: 5432
```
4. In the same `.credentials` directory, write and save a YAML file called `remote_db_creds.yaml` containing the credentials for the relevant AWS RDS database holding the data for the central orders table and the users dimension table.

5. In the same directory, write and save a JSON file called `api_config.json` containing a key-value pair storing the `x-api-key` needed for the API requests header.

6. If you're working from an IDE, make sure you've selected the miniconda environment created earlier as your interpreter path. If running the program from your computer's terminal, make sure you've activated the environment on the command line:
```
$ conda activate multinational-retail-data-centralisation
```
7. You are now ready to install the database on your local postgres server. This can be done from the parent directory by running:
```
$ python -m db_setup
```
## [Usage](#usage)
The completed database is composed of 5 dimension tables that relate to a single-source-of-truth table at its centre, which contains the definitive reference data for the organisation's retail orders.

<p style="text-align: center">
    <img src="readme-images/final-database-schema-erd.png" alt="The completed database ERD">
</p>

SQL queries can be run on the centralised data from within your chosen graphical database management interface, e.g. `pgAdmin 4`, or from an IDE such as `VSCode` using a driver to connect to the database like VS Code's `SQLTools` extension, or from the command line using `psql`.
See the [key findings](#findings-of-data-analysis) below for examples of the database in action.

## [Findings of Data Analysis](#findings-of-data-analysis)

### 1. How many stores does the business have and in which country?
The operations team wanted to know which countries they currently operate in and which country now has the most stores. The results showed their brick-and-mortar stores are located across the UK, Germany and the US, with most of their stores based in the UK.

| country | total_no_stores |
| --------- | -------------
| GB      |             266 |
| DE      |             141 |
| US      |              34 |

### 2. Which locations currently have the most stores?
The business stakeholders were looking to close some stores before opening more in other locations. To help in their decision of where to close stores, they wanted to know which locations had the most stores at present.

The query produced the following locations as having the most stores:

| locality     | total_no_stores |
| ------------ | --------------- |
| Chapletown   |              14 |
| Belper       |              13 |
| Bushey       |              12 |
| Exeter       |              11 |
| Arbroath     |              10 |
| High Wycombe |              10 |
| Rutherglen   |              10 |

### 3. Which months of the year produce the largest amounts of sales?
To find out which months of the year have historically produced the most sales, I joined the products and date times dimensions tables onto the orders table and calculated the overal total revenues grouped by month.

The results showed that the month which had historically produced the most sales revenue was August, followed by January, then October.

| total_sales | month |
| ----------- | ----- |
|  673,295.68 |     8 |
|  668,041.45 |     1 |
|  657,335.84 |    10 |
|  650,321.43 |     5 |
|  645,741.70 |     7 |
|  645,463.00 |     3 |

### 4. How many sales are happening online vs offline?

The company is looking to increase its online sales. They wanted to know how many sales were happening online compared to offline. I calculated how many products have been sold and the amount of sales made (in GBP) for online and offline purchases across the company's sales data history.

The results showed that their offline sales have produced over three times the number and total revenue of sales than their web store.

| numbers_of_sales | product_quantity_count | location |
| ---------------- | ---------------------- | -------- |
|           26,957 |                107,739 | Web      |
|           93,166 |                374,047 | Offline  |

### 5. What percentage of sales come through each type of store?
The sales team wanted to know which of the different store types generated the most revenue so that they could decide where to focus their efforts.
I calculated the total sales revenue (in GBP) and the percentage of sales coming from each of the different store types.

| store_type  |  total_sales | percentage_total(%) |
| ----------- | ------------ | ------------------- |
| Local       | 3,440,896.52 |               44.56 |
| Web Portal  | 1,726,547.05 |               22.36 |
| Super Store | 1,224,293.65 |               15.85 |
| Mall Kiosk  |   698,791.61 |                9.05 |
| Outlet      |   631,804.81 |                8.18 |

### 6. Which month in which year produced the highest cost of sales?
The company stakeholders wanted assurances that the company has been doing well recently.
I ran a query to find out which months in which years have had the most sales historically. (The latest sales data is from 2022.) The results show that none of the last three years feature in the company's most profitable months.

| total_sales |  year | month |
| ----------- | ----- | ----- |
|   27,936.77 | 1,994 |     3 |
|   27,356.14 | 2,019 |     1 |
|   27,091.67 | 2,009 |     8 |
|   26,679.98 | 1,997 |    11 |
|   26,310.97 | 2,018 |    12 |
|   26,277.72 | 2,019 |     8 |
|   26,236.67 | 2,017 |     9 |
|   25,798.12 | 2,010 |     5 |
|   25,648.29 | 1,996 |     8 |
|   25,614.54 | 2,000 |     1 |

### 7. What is the staff headcount?
The operations team wanted to know the overall staff numbers in each location around the world. I performed a query to determine the staff numbers in each of the countries the company sells in.

| total_staff_numbers | country_code |
| ------------------- | ------------ |
|              13,307 | GB           |
|               6,123 | DE           |
|               1,384 | US           |

### 8. Which German store is selling the most?
The sales team was looking to expand their territory in Germany. I ran a query to determine which type of store had generated the most sales in Germany across the company's sales history.

|  total_sales | store_type  | country_code |
| ------------ | ----------- | ------------ |
|   198,373.57 | Outlet      | DE           |
|   247,634.20 | Mall Kiosk  | DE           |
|   384,625.03 | Super Store | DE           |
| 1,109,909.59 | Local       | DE           |

*(For most of the results above, a query focussing on a window of more recent sales is likely to provide more actionable information, since these queries have been run on sales data spanning three decades.)*

### 9. How quickly is the company making sales?
The sales team wanted an accurate metric for how quickly the company is making sales. I determined the average time taken between each sale, grouped by year.

| year | actual_time_taken                                            |
| ---- | ------------------------------------------------------------ |
| 2013 | {"hours":2,"minutes":17,"seconds":12,"milliseconds":300.182} |
| 1993 | {"hours":2,"minutes":15,"seconds":35,"milliseconds":857.327} |
| 2002 | {"hours":2,"minutes":13,"seconds":50,"milliseconds":412.529} |
| 2022 | {"hours":2,"minutes":13,"seconds":6,"milliseconds":313.993}  |
| 2008 | {"hours":2,"minutes":13,"seconds":2,"milliseconds":803.08}   |
| 2008 | {"hours":2,"minutes":13,"seconds":2,"milliseconds":803.08}   |
| 1995 | {"hours":2,"minutes":12,"seconds":58,"milliseconds":972.925} |
| 2016 | {"hours":2,"minutes":12,"seconds":58,"milliseconds":124.905} |
| 2011 | {"hours":2,"minutes":12,"seconds":19,"milliseconds":17.623}  |
| 2020 | {"hours":2,"minutes":12,"seconds":3,"milliseconds":535.204}  |
| 2012 | {"hours":2,"minutes":11,"seconds":58,"milliseconds":69.104}  |
| 2021 | {"hours":2,"minutes":11,"seconds":56,"milliseconds":199.548} |
| 2009 | {"hours":2,"minutes":11,"seconds":18,"milliseconds":413.543} |
| 2010 | {"hours":2,"minutes":11,"seconds":13,"milliseconds":985.272} |
| 2007 | {"hours":2,"minutes":11,"seconds":8,"milliseconds":939.122}  |
| 1999 | {"hours":2,"minutes":11,"seconds":6,"milliseconds":563.482}  |
| 1996 | {"hours":2,"minutes":10,"seconds":59,"milliseconds":163.022} |
| 2000 | {"hours":2,"minutes":10,"seconds":54,"milliseconds":498.758} |
| 2019 | {"hours":2,"minutes":10,"seconds":47,"milliseconds":79.871}  |
| 1994 | {"hours":2,"minutes":10,"seconds":43,"milliseconds":552.599} |
| 2001 | {"hours":2,"minutes":10,"seconds":38,"milliseconds":953.766} |
| 2018 | {"hours":2,"minutes":10,"seconds":35,"milliseconds":807.157} |
| 2004 | {"hours":2,"minutes":10,"seconds":32,"milliseconds":996.037} |
| 2006 | {"hours":2,"minutes":10,"seconds":20,"milliseconds":328.044} |
| 2014 | {"hours":2,"minutes":10,"seconds":7,"milliseconds":507.304}  |
| 1997 | {"hours":2,"minutes":9,"seconds":58,"milliseconds":199.308}  |
| 2015 | {"hours":2,"minutes":9,"seconds":37,"milliseconds":417.016}  |
| 1992 | {"hours":2,"minutes":9,"seconds":32,"milliseconds":62.921}   |
| 2005 | {"hours":2,"minutes":8,"seconds":59,"milliseconds":660.53}   |
| 2017 | {"hours":2,"minutes":8,"seconds":46,"milliseconds":828.025}  |
| 2003 | {"hours":2,"minutes":8,"seconds":45,"milliseconds":491.916}  |
| 1998 | {"hours":2,"minutes":8,"seconds":7,"milliseconds":956.363}   |

## [Licence](#licence)

This project was supervised and is owned by [AiCore](https://www.theaicore.com/), a specialist AI & Data career accelerator whose focus is on building experience through real-world, industry-grade projects and applications.