from .pg_connect import pg_connect, IntegrityError, dt, Error
from prefect import get_run_logger
from pandas import read_excel, DataFrame


INSERT_STK_SQL = """
    INSERT INTO product_stock_agl (product_id, qty_available_agl,create_date,write_date, create_uid, write_uid, default_code)
    VALUES (
        COALESCE((SELECT id FROM product_product WHERE default_code = %s LIMIT 1), NULL),
        %s,%s,%s,%s,%s,%s
    )
"""

UPDATE_STK_SQL = """
    UPDATE product_stock_agl
    SET qty_available_agl = %s,
        write_date = %s,
        product_id = COALESCE((SELECT id FROM product_product WHERE default_code = %s LIMIT 1), NULL)
    WHERE id = (SELECT id FROM product_stock_agl WHERE default_code = %s
    AND qty_available_agl = %s ORDER BY product_id DESC LIMIT 1)
"""

DELETE_STK_SQL = """
    DELETE FROM product_stock_agl
    WHERE id = (
        SELECT id FROM product_stock_agl
        WHERE default_code = %s AND qty_available_agl = %s
        ORDER BY product_id DESC LIMIT 1)
"""


def upload_stock(df: DataFrame):
    log = get_run_logger()
    with pg_connect() as conn:
        start = dt.datetime.now()
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                state = row.get('state')
                if state == 'insert':
                    insert_stock(row, conn, cur, log)
                elif state == 'update':
                    update_stock(row, conn, cur, log)
                elif state == 'delete':
                    delete_stock(row, conn, cur, log)
        return dt.datetime.now() - start

def insert_stock(row, conn, cur, log):
    try:
        cur.execute("SELECT id FROM product_stock_agl WHERE default_code = %s AND create_date = %s", (row["ART_CODE"], row["time"]))
        existing = cur.fetchone()
        if existing:
            log.info(f"Stock Product with code {row['ART_CODE']} already exists — skipping insert.")
            return
        cur.execute(
            INSERT_STK_SQL,
            (
                row["ART_CODE"],
                row["STK_REEL"],
                row["time"],
                row["time"],
                6,
                6,
                row["ART_CODE"]
            )
        )
    except Error as e:
        conn.rollback()
        log.error(f"Can't insert stock for: {row} -> {e}")

def update_stock(row, conn, cur, log):
    try:
        cur.execute("SELECT id FROM product_stock_agl WHERE write_date = %s", (row["time"],))
        existing = cur.fetchone()
        if existing:
            log.info(f"Stock Product with code {row['ART_CODE']} already exists — skipping write.")
            return
        cur.execute(
            UPDATE_STK_SQL,
            (
                row["STK_REEL"],
                # row["ART_CODE"],
                row["time"],
                row["ART_CODE"],
                row["ART_CODE"],
                row["PAST_STK_REEL"],
            )
        )
    except Error as e:
        conn.rollback()
        log.error(f"Can't update stock for: {row} -> {e}")

def delete_stock(row, conn, cur, log):
    try:
        cur.execute(
            DELETE_STK_SQL,
            (
                row["ART_CODE"],
                row["STK_REEL"]
            )
        )
    except Error as e:
        conn.rollback()
        log.error(f"Can't delete stock for: {row} -> {e}")
