import logging
import time

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
        logging.INFO("json loaded")
        jsontodf.raceresults()

        trackdata = jsontodf.track_data()
        trackdf = jsontodf.loadtodf(trackdata)
        print(trackdf.shape)

        if trackdf is None or len(trackdf) == 0:
            logging.error(f"Transformation failed or returned empty dataset for {API_url}")

            return False

        # 3. transform 2
        #df = TransformDf(trackdf)


        # 4. load

        #Example usage(import):
        inserter = PostgresLoader()
        inserter.get_connection_details()
        inserter.create_engine()
        #df = pd.read_csv('your_data.csv')  # or however you get your DataFrame
        inserter.insert_dataframe(trackdf)

    except Exception as e:
        logging.exception(f"ETL pipeline for {API_url} failed: {e}")

        return False



def main():
    apis = ['https://f1fantasytools.com/api/statistics/2024']  # define your different API sources

    for api in apis:
        success = RunEtlPipeline(api)
        if not success:
            logging.error(f"Stopping ETL process due to failure in {api}.")
            break
        time.sleep(2)  # optional pause between API runs

    logging.info("All ETL jobs complete.")


if __name__ == "__main__":
    main()
