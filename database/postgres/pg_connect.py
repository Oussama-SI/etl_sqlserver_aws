from psycopg2 import connect, IntegrityError, Error, OperationalError
from psycopg2.extras import RealDictCursor
import datetime as dt

PG_CONNECT = "dbname='DB_test' user='openpg' password='openpgpwd'"

def pg_connect():
     try:
          conn = connect(PG_CONNECT)
          conn.autocommit = True
          return conn
     except OperationalError as e:
          # print(e)
          raise e
     except Exception as E:
          # print("un erreur occupé !", E)
          raise E
