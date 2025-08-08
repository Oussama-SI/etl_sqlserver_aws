from prefect import flow, task, get_run_logger
from prefect.server.schemas.schedules import IntervalSchedule
from datetime import datetime

from database.sqlserver.get_triggers import (
     get_connect, 
     query_as_dataframe, 
     cleaning_trigger_table, 
     STOCK_TRIGGER, 
     FAMILE_TRIGGER, 
     ARTICLE_TRIGGER,
)
from database.postgres import upload_article, upload_famille, upload_stk_reel

from datetime import datetime, timedelta
import pandas as pd


def filter_recent_rows(df: pd.DataFrame, column: str = 'time', minutes: int = 11) -> pd.DataFrame:
    """
    Filtre les lignes dont la valeur dans `column` est dans les X dernières minutes.

    :param df: DataFrame source
    :param column: Nom de la colonne de type datetime
    :param minutes: Fenêtre temporelle en minutes +1 pour éviter une perte des données
    :return: DataFrame filtré
    """
    if df.empty:
         return None
    now = datetime.now()
    threshold = now - timedelta(minutes=minutes)
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

### Trigger cleaning
@task
def clean_famille_trigger():
    return cleaning_trigger_table(get_connect(), 'trigger_famile')

@task
def clean_article_trigger():
    return cleaning_trigger_table(get_connect(), 'trigger_articles')

@task
def clean_stock_trigger():
    return cleaning_trigger_table(get_connect(), 'trigger_stock')


@flow(name="sorecom_pipeline")
def sorecom_pipeline():
    logger = get_run_logger()
    total_durations = []

    famille_df = get_famile_changes()
    article_df = get_article_changes()
    stock_df = get_stock_changes()

    if famille_df is not None and not famille_df.empty:
        fam_duration = upload_famille.upload_famile(famille_df)
        # logger.info(famille_df)
        total_durations.append(fam_duration)
        logger.info(f"Famille traitée en : {fam_duration}")
    else:
        logger.info("🔹 Aucun changement détecté pour Famille.")


    if article_df is not None and not article_df.empty:
        art_duration = upload_article.upload_product(article_df)
        # logger.info(article_df)
        total_durations.append(art_duration)
        logger.info(f"Article traité en : {art_duration}")
    else:
        logger.info("🔹 Aucun changement détecté pour Articles.")

    if stock_df is not None and not stock_df.empty:
        stk_duration = upload_stk_reel.upload_stock(stock_df)
        # logger.info(stock_df)
        total_durations.append(stk_duration)
        logger.info(f"Stock traité en : {stk_duration}")
    else:
        logger.info("🔹 Aucun changement détecté pour stock.")

    if total_durations:
        total_time = somme_time(*total_durations)
        logger.info(f"⏱️ Pipeline exécuté en : {total_time}")
    else:
        logger.info("Aucune donnée à traiter.")

@flow(name="cleanup_triggers_flow")
def cleanup_triggers_flow():
    clean_famille_trigger()
    clean_article_trigger()
    clean_stock_trigger()


if __name__ == "__main__":
    '''
    For Test first push in prefect server ,
    running: $ python main.py
    '''
    sorecom_pipeline.serve(
        name="sorecom-deployment",
        cron="*/10 * * * *",
        tags=["default"],
        # pause_on_shutdown=False
        # work_pool_name="default-agent-pool" 
    )
