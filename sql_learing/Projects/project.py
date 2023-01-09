import psycopg2
from sqlalchemy import create_engine
import pandas
import numpy

# database details
hostname = 'localhost'
username = 'postgres'
password = 'pass'
database = 'Data_engineering'


# create a dataframe object to capture all of the data from a table
def createDataFrame(connection, table):
    query = f"SELECT * FROM {table}"
    data_frame = pandas.read_sql_query(query, connection)
    return data_frame


# Function to modify table dataframe
def manipulateData(dataframe):
    # Replace any None entries in the column with TX
    dataframe['stat_cd'] = dataframe['stat_cd'].fillna('TX')

    # create and add commissions column and values to the dataframe
    comm_lst = []
    for i in dataframe["tran_ammt"]:
        comm_lst.append(i * 4)

    dataframe["commission"] = comm_lst

    # Final check to see if null values still exist
    for i in dataframe["stat_cd"]:
        if i is None:
            return "ERROR: Some values in the column are still NULL"

    return dataframe


def createTable(dataframe, connection, tablename):
    """
    Function using the pandas .to_sql function and the sqlalchemy create_engine function to create a new table with
    the modified values
    """
    engine = create_engine(f"postgresql://{username}:{password}@{hostname}:5432/{database}")
    dataframe.to_sql(tablename, engine, "cards_ingest", "replace")
    print(f"{tablename} successfully inserted into  the {database} database")
    return 1


def tableCompare(connection, table1, table2):
    print("Checking to see if both tables are the same length...")

    # Create the two table dataframes to compare
    table_frame1 = createDataFrame(connection, table1)
    table_frame2 = createDataFrame(connection, table2)

    # Check to see if they have the same number of records
    if len(table_frame1) == len(table_frame2):
        return "Success: The records of both tables are the same length"
    else:
        return "Error: tables are not the same length"


myConnection = psycopg2.connect(host=hostname, user=username, password=password, dbname=database)

if myConnection:
    print("Server successfully accessed")

dframe = createDataFrame(myConnection, "cards_ingest.tran_fact")
modified_frame = manipulateData(dframe)

createTable(modified_frame, myConnection, "tran_fact2")

print(tableCompare(myConnection, "cards_ingest.tran_fact", "cards_ingest.tran_fact2"))

myConnection.close()
