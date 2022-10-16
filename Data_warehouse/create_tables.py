import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This function will drop pre-existing tables  listed in the drop_table_queries array
    to ensure that the new tables can be created without throwing an error.

    Parameters:
        cur: psycopg2 connection to the database
        conn: cursor to interact with the database

    Returns:
        None

   """

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function will create tables listed in the create_table_queries array.

    Parameters:
        cur: psycopg2 connection to the database
        conn: cursor to interact with the database

    Returns:
        None
    """

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function will create the database connection and associated cursor and then drop tables 
    if they exist and create new tables for the start of the data pipeline.

    Parameters:
        None

    Returns:
        None
    """


    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()