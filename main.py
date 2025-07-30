from prefect import flow, task, get_run_logger
from prefect.server.schemas.schedules import IntervalSchedule
from datetime import datetime

from database.sqlserver.get_triggers import get_connect, query_as_dataframe, STOCK_TRIGGER, FAMILE_TRIGGER, ARTICLE_TRIGGER
from database.postgres import upload_article, upload_famille, upload_stk_reel

from datetime import datetime, timedelta
import pandas as pd


def filter_recent_rows(df: pd.DataFrame, column: str = 'time', minutes: int = 5) -> pd.DataFrame:
    """
    Filtre les lignes dont la valeur dans `column` est dans les X dernières minutes.

    :param df: DataFrame source
    :param column: Nom de la colonne de type datetime
    :param minutes: Fenêtre temporelle en minutes
    :return: DataFrame filtré
    """
    if df.empty:
         return None
    now = datetime.now()
    threshold = now - timedelta(minutes=minutes, hours=2)
    df[column] = pd.to_datetime(df[column], errors='coerce')
    return df[df[column] >= threshold].copy()
    
def somme_time(*args: timedelta) -> timedelta:
    if not args:
        raise ValueError("Aucune durée fournie pour somme_time()")
    return sum(args, timedelta())

@task
def get_famile_changes():
     fam_data = query_as_dataframe(get_connect(), FAMILE_TRIGGER)
     return filter_recent_rows(fam_data)

@task
def get_article_changes():
     art_data = query_as_dataframe(get_connect(), ARTICLE_TRIGGER)
     return filter_recent_rows(art_data)

@task
def get_stock_changes():
     stk_data = query_as_dataframe(get_connect(), STOCK_TRIGGER)
     return filter_recent_rows(stk_data)

# @flow
# def sorecom_pipeline():
# #     if df is None or df.empty:
# #         print(f" Aucun changement détecté pour {entity_name}")
# #         return
#     logging = get_run_logger()
#     time : datetime
#     # if (df['state'] == 'create').any():
#     if get_famile_changes():
#         fam_time += upload_famille.upload_famile(get_famile_changes())
#         logging.info(f"Famile bien traiter dans : {fam_time}")

#     # if (df['state'] == 'delete').any():
#     if get_article_changes():
#         art_time = upload_article.upload_product(get_article_changes())
#         logging.info(f"Famile bien traiter dans : {art_time}")

#     # if (df['state'] == 'update').any():
#     if get_stock_changes():
#         stk_time = upload_stk_reel.update_product_stk(get_stock_changes())
#         logging.info(f"Famile bien traiter dans : {stk_time}")

#     time = somme_time(stk_time, art_time, fam_time)
#     logging.info(f"pipline exécute ne {time}")

@flow(name="sorecom_pipeline")
def sorecom_pipeline():
    logger = get_run_logger()
    total_durations = []

    famille_df = get_famile_changes()
    article_df = get_article_changes()
    stock_df = get_stock_changes()

    if famille_df is not None and not famille_df.empty:
        fam_duration = upload_famille.upload_famile(famille_df)
        total_durations.append(fam_duration)
        logger.info(f"Famille traitée en : {fam_duration}")
    else:
        logger.info("🔹 Aucun changement détecté pour Famille.")


    if article_df is not None and not article_df.empty:
        art_duration = upload_article.upload_product(article_df)
        total_durations.append(art_duration)
        logger.info(f"Article traité en : {art_duration}")
    else:
        logger.info("🔹 Aucun changement détecté pour Articles.")

    if stock_df is not None and not stock_df.empty:
        stk_duration = upload_stk_reel.update_product_stk(stock_df)
        total_durations.append(stk_duration)
        logger.info(f"Stock traité en : {stk_duration}")
    else:
        logger.info("🔹 Aucun changement détecté pour stock.")

    if total_durations:
        total_time = somme_time(*total_durations)
        logger.info(f"⏱️ Pipeline exécuté en : {total_time}")
    else:
        logger.info("Aucune donnée à traiter.")


# ------------------- Flow -------------------
# @flow
# def sorecom_pipeline():
#     """
#     Pipeline d'intégration SOFECOM → PostgreSQL via triggers.
#     Détecte les changements récents (moins de 5 minutes) et applique les CUD.
#     """
#     fam_changes = get_famile_changes()
#     art_changes = get_article_changes()
#     stk_changes = get_stock_changes()

#     handle_changes(fam_changes, "famille")
#     handle_changes(art_changes, "article")
#     handle_changes(stk_changes, "stock")

# Deployment.build_from_flow(
#     flow=sorecom_pipeline,
#     name="sorecom-5min-schedule",
#     schedule=IntervalSchedule(interval=timedelta(minutes=5)),
#     work_queue_name="default"
# )

if __name__ == "__main__":
    sorecom_pipeline.serve(
        name="sorecom-deployment",
        cron="*/5 * * * *",       # ex : toutes les 5 minutes
        tags=["default"],
        # pause_on_shutdown=False
        # work_pool_name="default-agent-pool" 
    )
