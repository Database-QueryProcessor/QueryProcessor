"""
This file is used to test equivalent sql queries against our esql queries fed into our custom query processor. 
"""

import psycopg2
import psycopg2.extras
import tabulate
from queries import sqlQuery


def query():

    connection_params = {
        'dbname': "carmengvargas",
        'host': "localhost",
        'port': "5432"
    }

    body = sqlQuery()

    conn = psycopg2.connect(**connection_params,
                            cursor_factory=psycopg2.extras.DictCursor)
    cur = conn.cursor()
    cur.execute(body)

    return tabulate.tabulate(cur.fetchall(),
                             headers="keys", tablefmt="psql")


def main():
    print(query())


if "__main__" == __name__:
    main()
