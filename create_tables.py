import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
from time import time


def drop_tables(cur, conn):
    for query in drop_table_queries:
        print(query)
        start = time()

        cur.execute(query)
        conn.commit()

        end = time()
        print("execution time = %s seconds" % round(end - start, 2))


def create_tables(cur, conn):
    for query in create_table_queries:
        print("\n{}".format(query.strip()))
        start = time()

        cur.execute(query)
        conn.commit()

        end = time()
        print("execution time = %s seconds" % round(end - start, 2))


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                            .format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
