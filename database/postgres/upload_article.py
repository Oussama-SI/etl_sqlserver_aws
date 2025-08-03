from pandas import read_excel, DataFrame
from json import dumps
from prefect import get_run_logger

from .pg_connect import pg_connect, IntegrityError, Error, RealDictCursor, dt

CREATE_TMP = """
    INSERT INTO product_template (
                    sequence, name, categ_id, uom_id, uom_po_id, create_uid, write_uid, type, service_tracking,
                    default_code, list_price, volume, weight, sale_ok, purchase_ok, active, can_image_1024_be_zoomed,
                    has_configurable_attributes, is_favorite, create_date, write_date, sale_delay, tracking, 
                    is_storable,responsible_id
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                RETURNING id
"""
UPDATE_TMP = """
    UPDATE product_template
    SET name = %s, categ_id = %s, list_price = %s, write_date = %s, active = %s
    WHERE default_code = %s
"""
DELETE_TMP = """
    DELETE FROM product_template
    WHERE default_code = %s
"""

CREATE_PRODUCT = """
                INSERT INTO product_product (
                    product_tmpl_id,create_uid,write_uid,default_code,active,
                    can_image_variant_1024_be_zoomed,write_date,create_date,agl_memo_url,image_hex
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
UPDATE_PRODUCT = """
    UPDATE product_product
    SET active = %s, agl_memo_url = %s, image_hex = %s
    WHERE default_code = %s
""" #no
DELETE_PRODUCT ="""
    DELETE FROM product_product
    WHERE default_code = %s
"""

CATEG ="""
    SELECT id from product_category
    where name = %s
    LIMIT 1
"""

# data = [['AMM542','FB','OUSSAMA KI TESTER',1,10000,None,None,'insert','2025-07-23 10:05:31.163'],
# ['AMM542','FB','TESTER',1,10000,None,None,'insert','2025-07-24 10:46:01.087'],
# ['AAAAAA','FB','OUSSAMA',1,5000,None,None,'insert','2025-07-25 16:59:11.680']]
# index = ['ART_CODE', 'FAR_CODE', 'ART_LIB', 'ART_DORT', 'ART_P_VTEB', 'ART_MEMO', 'ART_IMAGE', 'state', 'time']
# df = DataFrame(data=data, columns=index)
# print(df)
# breakpoint()

def dict_name(value : str):
    return dumps({
    "en_US": value,
    "fr_FR": value
    })

def get_categ_id(conn, sql ,code) -> int:
     log = get_run_logger()
     with conn.cursor(cursor_factory=RealDictCursor) as curs:
         try:
            curs.execute(sql, (code,))
            result = curs.fetchone()
            # print(result['id'])
            if result:
                return result['id']
            else:
                raise ValueError(f"Aucune catégorie trouvée pour : {code}")
         except (Exception, IntegrityError) as e:
            log.exception(f'No categorie code found: {code}')
            return None

def upload_product(df : DataFrame):
     log = get_run_logger()
     with pg_connect() as conn:
          start = dt.datetime.now()    
          with conn.cursor() as cur:
               for _, row in df.iterrows():
                    if row['state'] == 'insert':
                         insert_product(row, conn, cur, log)
                    if row['state'] == 'update':
                         update_product(row, conn, cur, log)
                    if row['state'] == 'delete':
                         delete_product(row, conn, cur, log)
          return dt.datetime.now() - start
         
# get_categ_id(CATEG, "V2")

def insert_product(row, conn, cur, log):
                try:
                    cur.execute("SELECT id FROM product_product WHERE default_code = %s", (row["ART_CODE"],))
                    existing = cur.fetchone()
                    if existing:
                        log.info(f"Product with code {row['ART_CODE']} already exists — skipping insert.")
                        return
                    # === Insertion dans product_template ===
                    cur.execute(
                        CREATE_TMP,
                        (
                            1,
                            dict_name(row["ART_LIB"]),                         # name
                            get_categ_id(conn, CATEG, row['FAR_CODE']),# categ_id
                            1,                # uom_id
                            1,                # uom_po_id
                            6,                # create_uid
                            6,                # write_uid
                            "consu",          # type
                            "no",             # service_tracking
                            row["ART_CODE"],  # default_code
                            row["ART_P_VTEB"],     # list_price
                            0.00,             # volume
                            0.00,             # weight
                            True,             # sale_ok
                            True,             # purchase_ok
                            not bool(row["ART_DORT"]),   # active
                            False,            # can_image_1024_be_zoomed
                            False,            # has_configurable_attributes
                            False,            # is_favorite
                            row['time'],      # create_date
                            dt.datetime.now(),# write_date
                            0,                # sale_delay
                            "none",           # tracking
                            True,             # is_storable
                            dumps({"1": 2})          # responsible_id
                            )
                    )
                    tmpl_id = cur.fetchone()[0]

                    # === Insertion dans product_product ===
                    cur.execute(
                        CREATE_PRODUCT,
                        (
                            tmpl_id,
                            6,  # create_uid
                            6,  # write_uid
                            row["ART_CODE"],
                            not bool(row["ART_DORT"]),
                            False,
                            row["time"],
                            row["time"],
                            row["ART_MEMO"] or None,
                            row["ART_IMAGE"] or None
                        )
                    )
                    # print(f"{row} : successfully inserted product")
                    # conn.commit()

                except Exception as e:
                    conn.rollback()
                    log.error(f"Create error at row {row} : {e}")
                    
def update_product(row, conn, cur, log):
                try:
                    cur.execute("SELECT id FROM product_product WHERE write_date = %s", (row["time"],))
                    existing = cur.fetchone()
                    if existing:
                        log.info(f"Product with code {row['ART_CODE']} already exists — skipping insert.")
                        return
                    # === Insertion dans product_template ===
                    cur.execute(
                        UPDATE_TMP,
                        (
                            dict_name(row["ART_LIB"]),                         # name
                            get_categ_id(conn, CATEG, row['FAR_CODE']) or None,# categ_id
                            row["ART_P_VTEB"],           # list_price
                            row['time'],                 # write_date
                            not bool(row["ART_DORT"]),   # activ
                            row["ART_CODE"],             # default_code
                        )
                    )
                    cur.execute(
                        UPDATE_PRODUCT,
                        (
                            not bool(row["ART_DORT"]),   # active
                            row["ART_MEMO"] or None,
                            row["ART_IMAGE"] or None,
                            row["ART_CODE"],             # default_code
                        )
                    )
                    # print(f"{row}: seccesfly updated")
                except Exception as e:
                    conn.rollback()
                    # print(f"Error at row {row}: {e}")
                    log.error(f"Upload erreur for : {row} = {e}")

def delete_product(row, conn, cur, log):
                try:
                    cur.execute("SELECT id FROM product_product WHERE code = %s", (row["ART_CODE"],))
                    result = cur.fetchone()
                    if result:
                        product_id = result[0]
                        cur.execute("""
                            UPDATE product_stock_agl
                            SET product_id = NULL
                            WHERE product_id = %s
                        """, (product_id,))
                    # === DELETE dans product_product et product_template ===
                    cur.execute(
                        DELETE_PRODUCT,
                        (row['ART_CODE'],)
                    )
                    cur.execute(
                        DELETE_TMP,
                        (row['ART_CODE'],)
                    )
                    # print(f"{row}: seccusfly deleted")
                except Exception as e:
                    conn.rollback()
                    # print(f"Error at row {row}: {e}")
                    log.error(f"Delete erreur for : {row} = {e}")
                
# print(upload_product(df))
