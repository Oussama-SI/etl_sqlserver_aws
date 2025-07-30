import os
from pandas import DataFrame
from pyodbc import connect, InternalError
import datetime

SQL_CONNECTION_STRING = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=localhost;"
    "Database=SOFECOM;"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
    "UID=sa;"
    "PWD=Allezmagne@2023"
)
def get_connect():
    try:
        conn = connect(SQL_CONNECTION_STRING)
        return conn
    except InternalError as e:
        raise e
    except Exception as e:
        print("Connection Error:", e)
    

# data = pd.read_excel("C:/Users/lenovo/Desktop/SOFECOM/table articles.xlsx")
# print(data.loc[283, 'ART_IMAGE'])

FAMILE_TRIGGER = "SELECT * FROM trigger_famile;"
ARTICLE_TRIGGER = "SELECT * FROM trigger_articles"
STOCK_TRIGGER = "SELECT * FROM trigger_stock"

SQL = """
    INSERT INTO SOFECOM.dbo.ARTICLES (ART_CODE,FAR_CODE,ART_LIB,ART_DORT,ART_P_VTEB,ART_MEMO,ART_IMAGE)
    VALUES (?, ?, ?, ?, ?, ?, ?)
"""

# def get_famile_trigger():
#     with conn.cursor() as cur:
#         cur.execute(FAMILE_TRIGGER)
#         recs = cur.fetchall()
#         cols = [column[0] for column in cur.description]
#         # for r in recs:
#         #     print(r)
#     return pd.DataFrame(recs, columns=cols)

def query_as_dataframe(conn, query) -> DataFrame:
    """
    Exécute une requête SQL et retourne le résultat sous forme de DataFrame.
    Gère automatiquement les cas où chaque ligne contient une liste encapsulée.
    
    :param conn: Objet de connexion pyodbc
    :param query: Requête SQL à exécuter
    :return: pd.DataFrame
    """
    with conn:    
        with conn.cursor() as cur:
            cur.execute(query)
            rowse = cur.fetchall()
            description = cur.description
            if not rowse:
                return DataFrame()
            
            cols = [col[0] for col in description]
            rows = [list(row) for row in rowse]

            return DataFrame(rows, columns=cols)
    
# print(query_as_dataframe(get_connect(), FAMILE_TRIGGER))

# for _, row in data.iterrows():
#     try:
#         cur.execute(
#             SQL,
#             (
#                row['ART_CODE'],
#                 row['FAR_CODE'],
#                 row['ART_LIB'],
#                 int(row['ART_DORT']) if not pd.isna(row['ART_DORT']) else None,
#                 float(row['ART_P_VTEB']) if not pd.isna(row['ART_P_VTEB']) else None,
#                 #row['ART_MEMO'] if not pd.isna(row['ART_MEMO']) else None,
#                 None,
#                 None
#             )
#         )
#     except Exception as e:
#         print(f"Insert failed for row {_}: {e}")

# try:
#     conn.commit()
# except Exception as e:
#     print("Commit failed:", e)

# cur.close()
# conn.close()
# print("Duration:", datetime.datetime.now() - start_time)
