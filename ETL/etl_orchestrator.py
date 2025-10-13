import logging
import time
import datetime
import re

from extract import fetch_f1_fantasy_data
from transform import F1FantasyFrameBuilder
from transform2 import TransformDf, PointTextColumnDeletion
from load import PostgresLoader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def RunEtlPipeline(API_url):
    """"
    Runs the etl pipeline for a given API
    """
    logging.info(f"Starting ETL pipeline for {API_url}")

    try:
        # 1. extract

        raw_data = fetch_f1_fantasy_data(API_url)
        logging.info(f"Extraction succesful for {API_url}")
        if raw_data is None or len(raw_data) == 0:
            logging.error(f"Extraction failed or returned no data for {API_url}")

            return False


        # 2. transform
        jsontodf = F1FantasyFrameBuilder(raw_data)
        jsontodf.loadjson()
        logging.info("json loaded")
        jsontodf.raceresults()

        trackdata, key = jsontodf.DriverResults(1)
        table=generate_table_name(key, 1,raw_data)
        print(table)
        print(len(trackdata))

        trackdf = jsontodf.loadtodf(trackdata)

        # 2.5 transform2
        #trackdf = TransformDf(trackdf)
        print(trackdf.shape)
        #print(trackdf)

        if trackdf is None or len(trackdf) == 0:
            logging.error(f"Transformation failed or returned empty dataset for {API_url}")

            return False




    #     # 4. load
        # inserter = PostgresLoader()
        # inserter.get_connection_details()
        # inserter.create_engine()
        # table_name = generate_table_name()
        # inserter.insert_dataframe(trackdf, table_name = table_name)

    except Exception as e:
        logging.exception(f"ETL pipeline for {API_url} failed: {e}")

        return False

def generate_table_name(data_type: str, roundnumber: int | str, data: dict | None = None) -> str:
    """
    Automatically generate a table name based on dataset type and metadata.

    Args:
        data_type (str): The logical name of the dataset, e.g. 'race_results', 'drivers'.
        data (dict, optional): JSON or dict with season/date fields.

    Returns:
        str: Clean table name suitable for SQL databases.
    """
    # Try to extract season/year
    season = None

    season = data.get("seasonResult", {}).get("season") or data.get("season")

    # Fall back to current year
    season = str(season or datetime.date.today().year)


    # Combine into table name
    table_name = f"{data_type}_{season}_{roundnumber}"

    # Ensure it's safe for SQL (remove invalid chars)
    table_name = re.sub(r"[^a-zA-Z0-9_]", "_", table_name).lower()

    return table_name



def main():
    apis = ['https://f1fantasytools.com/api/statistics/2024']  # define your different API sources



    for i in range(24):
        success = RunEtlPipeline(apis)
        if not success:
            logging.error(f"Stopping ETL process due to failure in {apis}.")
            break
        time.sleep(2)  # optional pause between API runs

    logging.info("All ETL jobs complete.")


if __name__ == "__main__":
    main()
