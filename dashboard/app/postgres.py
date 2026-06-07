import psycopg2
import pandas as pd
import os

def run_postgres_query(query, dbname="postgres", user="postgres", host="localhost", port=5432, password="password"):
    conn = psycopg2.connect(dbname=dbname, user=user, host=host, port=port, password=password)
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()
    return pd.DataFrame(rows, columns=colnames)