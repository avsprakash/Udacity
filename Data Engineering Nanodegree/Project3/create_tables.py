import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Description: This function invokes drops all the tables using the queries in the list drop_table_queries.

    Arguments:
        cur: the cursor object. 
        conn: connection to the Sparkify database.

    Returns:
        None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Description: This function creates all the tables using the queries in the list create_table_queries.

    Arguments:
        cur: the cursor object. 
        conn: connection to the Sparkify  database.

    Returns:
        None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        


def main():
    """
    Description: This is the main function where the processing starts when this python script runs.
                 It connects to the sparkifydb and creates a connection to it. It calls the drop_tables and 
                 create_tables functions.

    Arguments:
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
