import pyodbc
import pandas as pd
import snowflake.connector

conn = snowflake.connector.connect(
user='jwick26143',
password='Radialhwy1825@',
account='fb74146.central-us.azure',
warehouse='COMPUTE_WH',
database='ABC',
schema='DB_CONFIG')


try:
    cur = conn.cursor()
    query = "SELECT SOURCE_OBJECT FROM INGEST_OBJECT_SQL"  # Adjust query as needed
    cur.execute(query)

    # Fetch results
    results = cur.fetchall()

    # Print the results
    table_list = [row[0] for row in results]

finally:
    # Clean up: close the cursor and the connection
    cur.close()
    conn.close()

table_list = ['Person.Person','HumanResources.Department', 'HumanResources.Employee', 'Sales.SalesPerson']


# Define connection parameters
server = 'DESKTOP-64SMG1I'  # e.g., 'localhost' or '192.168.1.1'
database = 'AdventureWorks2019'  # e.g., 'test_db'
username = 'sa'
password = 'Talloaks612@'

# Create a connection string
connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'



conn = pyodbc.connect(connection_string)
cursor = conn.cursor()

# Write a SELECT query
query = 'SELECT * FROM Sales.SalesPerson'  # Modify YourTableName with your table's name

# Execute the query
cursor.execute(query)


df = pd.DataFrame.from_records(rows, columns=columns)


df


## Creating a function that takes a table name and then returns the table data in the dataframe form.
import pyodbc
import pandas as pd

def fetch_table_data(table_name):
    """
    Fetches data from a given table in SQL Server and returns it as a pandas DataFrame.

    Parameters:
    table_name (str): The name of the table to query.

    Returns:
    pd.DataFrame: A DataFrame containing the data from the specified table.
    """
# Define connection parameters
    server = 'DESKTOP-64SMG1I'  # e.g., 'localhost' or '192.168.1.1'
    database = 'AdventureWorks2019'  # e.g., 'test_db'
    username = 'sa'
    password = 'Talloaks612@'

    # Create a connection string
    connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

    try:
        # Establish connection to the SQL Server
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        # Write a dynamic SELECT query based on the table name
        query = f'SELECT  * FROM {table_name}'  # Fetching records

        # Execute the query
        cursor.execute(query)

        # Fetch the column names
        columns = [column[0] for column in cursor.description]

        # Fetch all rows
        rows = cursor.fetchall()

        # Convert the result to a pandas DataFrame
        df = pd.DataFrame.from_records(rows, columns=columns)

        # Return the DataFrame
        return df

    except pyodbc.Error as e:
        print(f"Error: {e}")
        return None

    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()



# Example usage
table_name = 'Sales.SalesPerson'  # Replace with the desired table name
df = fetch_table_data(table_name)

df



import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


snowflake_conn = snowflake.connector.connect(
        user='jwick26143',
        password='Radialhwy1825@',
        account='fb74146.central-us.azure',
        warehouse='COMPUTE_WH',
        database='ABC',
        schema='DL_SQLSERVER')


success, nchunks, nrows, _ = write_pandas(
        conn=snowflake_conn,
        df=df,table_name = table_name,
        auto_create_table = True,
        schema='DL_SQLSERVER', overwrite=True ) # Optional, in case schema is different


table_name



## Creating a function that takes the dataframe and snowflake table name as input parameters and then loads into snowflake.
def load_dataframe_to_snowflake(df, snowflake_table_name):
    # Define Snowflake connection parameters
    snowflake_conn = snowflake.connector.connect(
        user='jwick26143',
        password='Radialhwy1825@',
        account='DMXZVDN.KR23391',
        warehouse='COMPUTE_WH',
        database='ABC',
        schema='DL_SQLSERVER'
    )

    try:
        # Load the DataFrame into the Snowflake table
        success, nchunks, nrows, _ = write_pandas(
            conn=snowflake_conn,
            df=df,
            table_name=snowflake_table_name,
            schema='DL_SQLSERVER'  # Optional, in case schema is different
        )

        if success:
            print(f"Successfully loaded {nrows} rows into Snowflake table {snowflake_table_name}.")
        else:
            print(f"Failed to load data into Snowflake table {snowflake_table_name}.")

    except snowflake.connector.errors.ProgrammingError as e:
        print(f"Error loading data into Snowflake: {e}")

    finally:
        snowflake_conn.close()

# Example usage:
table_name = 'YourTableName'  # SQL Server table name
df = fetch_table_data(table_name)

if df is not None:
    load_dataframe_to_snowflake(df, 'SnowflakeTableName')




