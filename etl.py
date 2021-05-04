import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from time import time


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print(query.strip())
        start = time()

        cur.execute(query)
        conn.commit()

        end = time()
        print("execution time = %s seconds\n" % round(end - start, 2))


def insert_tables(cur, conn):
    for query in insert_table_queries:
        print("\n{}".format(query.strip()))
        start = time()

        cur.execute(query)
        conn.commit()

        end = time()
        print("execution time = %s seconds" % round(end - start, 2))


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} "
                            "port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
