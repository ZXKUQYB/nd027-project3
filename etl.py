import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads data from JSON files stored in a S3 bucket hosted by Udacity into staging tables.

    Args:
        (psycopg2.extensions.cursor) cur - cursor object using the connection created by psycopg2
        (psycopg2.extensions.connection) conn - connection handle created by psycopg2
    Returns:
        None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserts data stored in staging tables into final tables.

    Args:
        (psycopg2.extensions.cursor) cur - cursor object using the connection created by psycopg2
        (psycopg2.extensions.connection) conn - connection handle created by psycopg2
    Returns:
        None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Reads AWS config from `dwh.cfg` file. 
    
    - Establishes connection with the sparkify database and gets cursor to it. 
    
    - Loads data into staging tables. 
    
    - Inserts data into final tables. 
    
    - Finally, closes the connection. 
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