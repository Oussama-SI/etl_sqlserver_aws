from pandas import read_excel, DataFrame
import datetime as dt
from prefect import get_run_logger

from .pg_connect import pg_connect, Error

__all__ = ['upload_famile']

SQL = """
    INSERT INTO product_category
    (create_uid,write_uid,name,complete_name,create_date,write_date,packaging_reserve_method,active)
    values (%s,%s,%s,%s,%s,%s,%s,%s)
"""
UPDATE = """
     UPDATE product_category
     SET complete_name = %s, write_date = %s, active = %s
     WHERE name = %s
"""
DELETE = """
     DELETE FROM product_category
     WHERE name = %s AND complete_name = %s
     RETURNING id
"""
WRITE = """
     UPDATE product_category
     SET parent_path = id::text || '/'
     WHERE parent_path IS NULL;
"""

CHECK_ART = """
     select 1 from product_product
"""

# with open("C:/Users/lenovo/Desktop/famile.xslx", "r") as file:
#      data = file.read()
# "parent_id""create_uid""write_uid""name""complete_name""parent_path""create_date""write_date"
# "packaging_reserve_method"
# df = read_excel("C:/Users/lenovo/Desktop/famille.xlsx")
#print(df)
# print(conn)

def upload_famile(df : DataFrame):
     log = get_run_logger()
     with pg_connect() as conn:
          start = dt.datetime.now()    
          with conn.cursor() as cur:
               for _, row in df.iterrows():
                    if row['state'] == 'create':
                         insert_categorie(row, conn, cur, log)
                    if row['state'] == 'update':
                         update_categorie(row, conn, cur, log)
                    if row['state'] == 'delete':
                         delete_categorie(row, conn, cur, log)
          return dt.datetime.now() - start

def insert_categorie(row, conn, cur, log):
     try:
          cur.execute("SELECT id FROM product_category WHERE name = %s", (row["FAR_CODE"],))
          existing = cur.fetchone()
          if existing:
               log.info(f"Product with code {row['FAR_CODE']} already exists — skipping insert.")
               return
          cur.execute(
          SQL,
          (
               2,
               2,
               row["FAR_CODE"],
               row["FAR_LIB"],
               row["time"],
               dt.datetime.now(),
               "partial",
               not bool(row["FAR_DORT"])
          )
          )
          # print(f" {row} : famille seccesfly created")
     except Error as E:
          conn.rollback()
          log.exception(f"Can't insert a fammile : {row}")
          # print(E)
     try:
          cur.execute(WRITE)
          # print("Updated parent_path for NULL rows")
     except Error as e:
          conn.rollback()
          log.exception(f"Can't update a path for categorie : {row}")
          # print("Error in UPDATE:", e)

def delete_categorie(row, conn , cur, log):
     try:
          cur.execute(
               DELETE,
               (
               row["FAR_CODE"],
               row["FAR_LIB"]
               )
          )
          # print(f"{row}: seccesfly updated")
     except Error as e:
          conn.rollback()
          log.exception(f"Can't delete a fammile : {row} => {e}")
          # print(E)
     
def update_categorie(row, conn, cur, log):
     try:
          cur.execute("SELECT id FROM product_category WHERE write_date = %s", (row["time"]))
          existing = cur.fetchone()
          if existing:
              log.info(f"Product Category with code {row['ART_CODE']} already exists — skipping update.")
              return
          cur.execute(
          UPDATE,
          (
               row["FAR_LIB"],
               row['time'],
               not bool(row["FAR_DORT"]),
               row["FAR_CODE"]
          )
          )
          # print(f"{row}: seccefly deleted")
     except Error as E:
          conn.rollback()
          log.exception(f"Can't update a fammile : {row}")
          # print(E)