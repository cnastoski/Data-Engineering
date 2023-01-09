import redshift.Projects.config.db_details as db
import psycopg2
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

engine = create_engine(URL.create(
    db.drivername,
    username=db.username,
    password=db.password,  # plain (unescaped) text
    host=db.hostname,
    port=db.port,
    database=db.database
))


def do_query(query: str, args: list):
    """
    execute postgresql query via postcopg2 on database configured in config/database_connection.py
    :param query: postgresql query string
    :param args: arguments for query string (see https://www.psycopg.org/docs/usage.html#the-problem-with-the-query-parameters)
    :return: pandas DataFrame for query result
    :rtype: pd.DataFrame
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


myConnection = psycopg2.connect(host=db.hostname, user=db.username, password=db.password, dbname=db.database,
                                port=db.port)

if myConnection:
    print("Server successfully accessed")

myConnection.close()
