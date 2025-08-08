import os
from pandas import DataFrame, read_csv, isna, read_excel
from pyodbc import connect, InternalError
from prefect import get_run_logger
import datetime

SQL_CONNECTION_STRING = (
    "Driver={SQL Server};"
    "Server=;"
    "Database=;"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
    "UID=;"
    "PWD="
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


FAMILE_TRIGGER = "SELECT * FROM dbo.trigger_famile order by time"
ARTICLE_TRIGGER = "SELECT * FROM dbo.trigger_articles order by time"
STOCK_TRIGGER = "SELECT * FROM dbo.trigger_stock order by time"



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

def cleaning_trigger_table(conn, table : str):
    with conn:
        with conn.cursor() as curs:
            try:
                # Construction de la requête : supprimer tout sauf les 20 derniers en table sql server
                query = f"""
                    DELETE FROM {table}
                    WHERE time NOT IN (
                    SELECT TOP 20 time
                    FROM {table}
                    ORDER BY time DESC)
                    """
                curs.execute(query)
                get_run_logger().info(f"Suppression réussie dans la table {table}.")
            except Exception as e:
                # get_run_logger().error(e)
                get_run_logger().error(f"Erreur SQL : {str(e)}")