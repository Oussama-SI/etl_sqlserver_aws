import os
from pandas import DataFrame, read_csv, isna, read_excel
from pyodbc import connect, InternalError
from prefect import get_run_logger
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
        conn = connect(SQL_CONNECTION_STRING, autocommit=True)
        return conn
    except InternalError as e:
        get_run_logger().info("Internal error in connecytion with sql server")
        return None
    except Exception as e:
        get_run_logger().exception(f"Connection Error: {e}")
        return None
    

# data = pd.read_excel("C:/Users/lenovo/Desktop/SOFECOM/table articles.xlsx")
# print(data.loc[283, 'ART_IMAGE'])

FAMILE_TRIGGER = "SELECT * FROM trigger_famile order by time desc"
ARTICLE_TRIGGER = "SELECT * FROM trigger_articles order by time desc"
STOCK_TRIGGER = "SELECT * FROM trigger_stock order by time desc"


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
