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
               log.info(f"Famile with code {row['FAR_CODE']} already exists — skipping insert.")
               return
          cur.execute(
          SQL,
          (
               2,
               2,
               row["FAR_CODE"],
               row["FAR_LIB"],
               row["time"],
               row["time"],
               "partial",
               not bool(row["FAR_DORT"])
          )
          )
          # print(f" {row} : famille seccesfly created")
     except Error as e:
          conn.rollback()
          log.error(f"Can't insert a fammile : {row} = {e}")
          # print(E)
     try:
          cur.execute(WRITE)
          # print("Updated parent_path for NULL rows")
     except Error as e:
          conn.rollback()
          log.error(f"Can't update a path for categorie : {row} = {e}")
          # print("Error in UPDATE:", e)

def delete_categorie(row, conn , cur, log):
     try:
          cur.execute("SELECT id FROM product_category WHERE name = %s", (
            row["FAR_CODE"],
          ))
          result = cur.fetchone()
          if result:
               category_id = result[0]
               cur.execute("""
                    UPDATE product_template
                    SET categ_id = 1
                    WHERE categ_id = %s
               """, (category_id,))
          
          cur.execute(
               DELETE,
               (
               row["FAR_CODE"],
               row["FAR_LIB"]
               )
          )
     except Error as e:
          conn.rollback()
          log.error(f"Can't delete a fammile : {row} = {e}")
     
def update_categorie(row, conn, cur, log):
     try:
          cur.execute("SELECT id FROM product_category WHERE write_date = %s", (row["time"],))
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
     except Error as e:
          conn.rollback()
          log.error(f"Can't update a fammile : {row} = {e}")
          # print(E)