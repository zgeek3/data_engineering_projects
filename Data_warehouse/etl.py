import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function will copy data from the S3 buckets into the staging tables.

    Parameters:
        cur: psycopg2 connection to the database
        conn: cursor to interact with the database

    Returns:
        None
    """

    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function will insert the data from the staging tables into the fact and 
    dimension tables.

    Parameters:
        cur: psycopg2 connection to the database
        conn: cursor to interact with the database

    Returns:
        None
    """

    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function will create the database connection and associated cursor and copy data
    from the S3 buckets into the staging tables and then insert data from the staging tables
    into the fact and dimension tables.

    Parameters:
        None

    Returns:
        None
    """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()