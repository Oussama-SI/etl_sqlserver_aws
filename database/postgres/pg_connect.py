from psycopg2 import connect, IntegrityError, Error, OperationalError
from psycopg2.extras import RealDictCursor
import datetime as dt
from prefect import get_run_logger

PG_CONNECT = "host='16.171.170.66' port='5432' dbname='SORECOM' user='sorecom' password='sorecom_aws_2025'"

def pg_connect():
     try:
          conn = connect(PG_CONNECT)
          conn.autocommit = True
          return conn
     except OperationalError as e:
          # print(e)
          get_run_logger().exception(e)
     except Exception as E:
          # print("un erreur occupé !", E)
          get_run_logger().error(E)
