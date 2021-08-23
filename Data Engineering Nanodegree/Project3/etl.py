import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Description: This function loads data in to the staging tables using the queries in the list copy_table_queries.

    Arguments:
        cur: the cursor object. 
        conn: connection to the Sparkify database.

    Returns:
        None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Description: This function inserts data in to the tables using the queries in the list insert_table_queries.

    Arguments:
        cur: the cursor object. 
        conn: connection to the Sparkify database.

    Returns:
        None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Description: This is the main function where the processing starts when this python script runs.
                 It connects to the sparkifydb and creates a connection to it. It calls the load_staging_tables and
                 insert_tables functions.

    Arguments:
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