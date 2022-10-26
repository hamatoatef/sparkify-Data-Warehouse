import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load staging tables from S3 buckets
    Attributes:
        cur: cursor to the database
        conn: connection object to the database
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Transform data in staging tables and insert them into analytics tables
    Attributes:
        cur: cursor to the database
        conn: connection object to the database
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Execute load_staging_tables function and insert_tables 
    close the connection
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