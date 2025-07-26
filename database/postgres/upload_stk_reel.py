from .pg_connect import pg_connect, IntegrityError, dt
from pandas import read_excel


STK_REEL_SQL = """
    UPDATE product_product
    SET qty_available_agl = %s,
        write_date = %s
    WHERE default_code = %s
"""

# conn = pg_connect()
# df = read_excel("C:/Users/lenovo/Desktop/SOFECOM/table stock.xlsx")

def update_product_stk(df):
     with pg_connect() as conn:
          start = dt.datetime.now()
          with conn.cursor() as curs:
               
               for _, row in df.iterrows():
                    try:
                         curs.execute(
                              STK_REEL_SQL,
                              (
                                   row['STK_REEL'],
                                   row['time'],
                                   row['ART_CODE']
                              )
                         )
                    except (Exception, IntegrityError) as E:
                         conn.rollback()
                         print(f"{_} : {E}")
                         raise E 
               conn.commit()

          return dt.datetime.now() - start
