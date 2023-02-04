import redshift.Projects.config.db_details as db
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
import re
import timeit

engine = create_engine(URL.create(
    db.drivername,
    username=db.username,
    password=db.password,  # plain (unescaped) text
    host=db.hostname,
    port=db.port,
    database=db.database
))


def do_query_string(query: str, args):
    """
    execute postgresql query via postcopg2 on database configured in config/database_connection.py :param query:
    postgresql query string :param args: arguments for query string (see
    https://www.psycopg.org/docs/usage.html#the-problem-with-the-query-parameters) :return: pandas DataFrame for
    query result :rtype: pd.DataFrame
    """
    conn = psycopg2.connect(host=db.hostname, user=db.username, password=db.password, dbname=db.database,
                            port=db.port)
    cur = conn.cursor()
    cur.execute(query, args)
    # pandas frame
    frame = None

    if cur.description:  # checks for returned table (if it has a description, the query has returned a table)
        data = cur.fetchall()
        column_names = [i[0] for i in cur.description]
        frame = pd.DataFrame(data, columns=column_names)
    cur.close()
    conn.commit()
    conn.close()

    return frame


def loadFromCSV(filePathName: str, table_name: str, schema: str):
    """
    execute postgresql query to append schema.table_name with frame data on connected database;
    frame is filled by data in filePathName csv file
    :param filePathName: csv filename to fill the pandas table with
    :param table_name: name of table to be overwritten
    :param schema: name of schema that table_name is in
    :param dtype: dict of data types for each column (defaults to varchar if nothing entered)
    :param if_exists: {‘fail’, ‘replace’, ‘append’} -- pandas to_sql parameter, defines behavior for frameToTable
    :return: None or Int
    :rtype: None if rows not returned, Int equal to number of rows affected (if integer returned for rows by sqlalchemy)
    """
    frame = pd.read_csv(filePathName)
    return do_frameToTable(frame, table_name, schema)


def do_frameToTable(frame, table_name: str, schema: str):
    """
    execute postgresql query to apend schema.table_name with frame data on connected database
    :param frame: pandas frame that will overwrite schema.table_name
    :param table_name: name of table to be overwritten
    :param schema: name of schema that table_name is in
    :param if_exists: {‘fail’, ‘replace’, ‘append’} -- pandas to_sql parameter, defines behavior for frameToTable
    :param method: method of inserting data into a table, defualt is None: 1 insert clause per row, multi: pass multiple values in a single insert clause
    :return: None or Int
    :rtype: None if rows not returned, Int equal to number of rows affected (if integer returned for rows by sqlalchemy)
    """

    return frame.to_sql(table_name, con=engine, schema=schema, if_exists='replace', index=False, method='multi')


def query_from_file(file_path):
    # Read the SQL file
    with open(file_path, 'r') as f:
        sql_query = f.read()

    # remove comments
    sql_query = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", sql_query)

    conn = psycopg2.connect(host=db.hostname, user=db.username, password=db.password, dbname=db.database,
                            port=db.port)
    cur = conn.cursor()
    # Execute the query
    cur.execute(sql_query)

    frame = None

    if cur.description:  # checks for returned table (if it has a description, the query has returned a table)
        data = cur.fetchall()
        column_names = [i[0] for i in cur.description]
        frame = pd.DataFrame(data, columns=column_names)
    cur.close()
    conn.commit()
    conn.close()
    return frame
