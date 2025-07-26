from pandas import read_excel, DataFrame
from json import dumps

from .pg_connect import pg_connect, IntegrityError, Error, RealDictCursor, dt

CREATE_TMP = """
    INSERT INTO product_template (
                    sequence, name, categ_id, uom_id, uom_po_id, create_uid, write_uid, type, service_tracking,
                    default_code, list_price, volume, weight, sale_ok, purchase_ok, active, can_image_1024_be_zoomed,
                    has_configurable_attributes, is_favorite, create_date, write_date, sale_delay, tracking, is_storable,
                    lot_valuated, responsible_id
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                RETURNING id
"""
UPDATE_TMP = """
    UPDATE product_template
    SET name = %s, categ_id = %s, lst_price = %s, write_date = %s, active = %s
    WHERE default_code = %s
""" #no
DELETE_TMP = """
    DELETE FROM product_template
    WHERE default_code = %s
"""

CREATE_PRODUCT = """
                INSERT INTO product_product (
                    product_tmpl_id,create_uid,write_uid,default_code,active,
                    can_image_variant_1024_be_zoomed,write_date,create_date
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
UPDATE_PRODUCT = """
    UPDATE product_product
    SET active = %s
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


def dict_name(value : str):
    return dumps({
    "en_US": value,
    "fr_FR": value
    })

def get_categ_id(conn, sql ,code) -> int:
     with conn.cursor(cursor_factory=RealDictCursor) as curs:
         try:
            curs.execute(sql, (code,))
            result = curs.fetchone()
            print(result['id'])
            if result:
                return result['id']
            else:
                raise ValueError(f"Aucune catégorie trouvée pour : {code}")
         except (Exception, IntegrityError) as e:
            print("Erreur dans la détection de la catégorie spécifique pour l'article créé.")
            raise e

def upload_product(df : DataFrame):
     with pg_connect() as conn:
          start = dt.datetime.now()    
          with conn.cursor() as cur:
               for _, row in df.iterrows():
                    if row['state'] == 'create':
                         insert_product(row, conn, cur)
                    if row['state'] == 'update':
                         update_product(row, conn, cur)
                    if row['state'] == 'delete':
                         delete_product(row, conn, cur)
          return dt.datetime.now() - start
         
# get_categ_id(CATEG, "V2")

def insert_product(row, conn, cur):
                try:
                    # === Insertion dans product_template ===
                    cur.execute(
                        CREATE_TMP,
                        (
                            1,
                            dict_name(row["ART_LIB"]),                         # name
                            get_categ_id(conn, CATEG, row['FAR_CODE']) or None,# categ_id
                            1,                # uom_id
                            1,                # uom_po_id
                            2,                # create_uid
                            2,                # write_uid
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
                            row['time'],# create_date
                            row['time'],# write_date
                            0,                # sale_delay
                            "none",           # tracking
                            True,             # is_storable
                            False,            # lot_valuated
                            dumps({"1": 2})          # responsible_id
                            )
                    )
                    tmpl_id = cur.fetchone()[0]  # récupère l'id de product_template inséré

                    # === Insertion dans product_product ===
                    cur.execute(
                        CREATE_PRODUCT,
                        (
                            tmpl_id,
                            1,  # create_uid
                            1,  # write_uid
                            row["ART_CODE"],
                            not bool(row["ART_DORT"]),
                            False,
                            dt.datetime.now(),
                            dt.datetime.now()
                        )
                    )
                    print(f"{row} : successfully inserted product")
                    # conn.commit()

                except Exception as e:
                    conn.rollback()
                    print(f"Error at row {row}: {e}")
                    raise e

    
def update_product(row, conn, cur):
                try:
                    # === Insertion dans product_template ===
                    cur.execute(
                        UPDATE_TMP,
                        (
                            dict_name(row["ART_LIB"]),                         # name
                            get_categ_id(conn, CATEG, row['FAR_CODE']) or None,# categ_id
                            row["ART_P_VTEB"],           # list_price
                            row['time'],                 # write_date
                            not bool(row["ART_DORT"]),   # active
                            row["ART_CODE"],             # default_code
                        )
                    )
                    cur.execute(
                        UPDATE_PRODUCT,
                        (
                            not bool(row["ART_DORT"]),   # active
                            row["ART_CODE"],             # default_code
                        )
                    )
                    print(f"{row}: seccesfly updated")
                except Exception as e:
                    conn.rollback()
                    print(f"Error at row {row}: {e}")
                    raise e

def delete_product(row, conn, cur):
                try:
                    # === DELETE dans product_product et product_template ===
                    cur.execute(
                        DELETE_PRODUCT,
                        (row['ART_CODE'])
                    )
                    cur.execute(
                        DELETE_TMP,
                        (row['ART_CODE'])
                    )
                    print(f"{row}: seccusfly deleted")
                except Exception as e:
                    conn.rollback()
                    print(f"Error at row {row}: {e}")
                    raise e
