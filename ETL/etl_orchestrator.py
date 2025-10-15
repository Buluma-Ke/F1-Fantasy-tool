import logging
import time
import datetime
import re
import traceback
import pandas as pd


from extract import fetch_f1_fantasy_data
from transform import F1FantasyFrameBuilder
from transform2 import TransformDf, PointTextColumnDeletion
from load import PostgresLoader, RunQuery

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def RunEtlPipeline(API_url):
    """"
    Runs the etl pipeline for a given API
    """
    logging.info(f"Starting ETL pipeline for {API_url}")

    # create database connection
    postgresdb = PostgresLoader()
    postgresdb.get_connection_details()
    postgresdb.create_engine()



    try:
        # 1️⃣ Extract
        raw_data = fetch_f1_fantasy_data(API_url)
        logging.info(f"Extraction successful for {API_url}")

        if raw_data is None or len(raw_data) == 0:
            logging.error(f"Extraction failed or returned no data for {API_url}")
            return False

        # 2️⃣ Transform
        try:
            logging.info("🚀 Starting data transformation process...")

            jsontodf = F1FantasyFrameBuilder(raw_data)
            jsontodf.loadjson()
            logging.info("✅ JSON successfully loaded into memory.")

            jsontodf.raceresults()
            logging.info("✅ Race results transformation complete.")

            track_data, key = jsontodf.track_data()
            logging.info("Track data extracted successfully for key: %s", key)

            trackdf = jsontodf.loadtodf(track_data)
            logging.info("Track data loaded into DataFrame (rows=%d, cols=%d)", len(trackdf), len(trackdf.columns))

            trackdf = TransformDf(trackdf)
            track_table = generate_table_name(key, 1, raw_data)
            logging.info("Track data transformed. Target table: %s", track_table)

            sprint_rounds = trackdf.loc[trackdf['sprint'].notna(), 'roundNumber'].tolist()
            logging.info("Sprint rounds detected: %s", sprint_rounds)

            season_result = raw_data.get("seasonResult", {})
            total_rounds = season_result.get("raceResults", [])

            total_rounds = len(total_rounds)
            logging.info("Total rounds found in raw_data: %d", total_rounds)

            # 3️⃣ Iterate over each round

            for i in range(1, total_rounds + 1):
                logging.info("---- 🏁 Processing round %d/%d ----", i, total_rounds)

                try:
                    # ---------------- DRIVER RESULTS ----------------
                    driverdata, key = jsontodf.DriverResults(i)
                    driverdf = jsontodf.loadtodf(driverdata)
                    driverdf = PointTextColumnDeletion(driverdf)
                    driverdf['team'] = driverdf['id'].str.split('_', expand=True)[0]
                    driver_table = generate_table_name(key, i, raw_data)
                    logging.info("Driver results processed. Table: %s", driver_table)

                    # Initialize anchortable in first iteration
                    if i == 1:
                        anchortable = driverdf[['id', 'abbreviation']].copy()

                    # Add any new drivers found in later rounds
                    for _, driver in driverdf[['id', 'abbreviation']].iterrows():
                        if driver['id'] not in anchortable['id'].values:
                            anchortable = pd.concat([anchortable, pd.DataFrame([driver])], ignore_index=True)
                            postgresdb.insert_dataframe(anchortable, table_name='drivers')
                            postgresdb.RunQuery('ALTER TABLE drivers ADD CONSTRAINT pk_drivers_id PRIMARY KEY (id);')


                    # ---------------- CONSTRUCTOR RESULTS ----------------
                    constructordata, key = jsontodf.ConstructorsResults(i)
                    print(len(constructordata))
                    const_df = jsontodf.loadtodf(constructordata)
                    const_df = PointTextColumnDeletion(const_df)
                    print(const_df.columns)
                    consructor_table = generate_table_name(key, i, raw_data)
                    logging.info("Constructor results processed. Table: %s", consructor_table)

                     # Initialize anchortable in first iteration
                    if i == 1:
                        anchortable_con = const_df[['id', 'abbreviation']].copy()

                    # Add any new constractor found in later rounds
                    for _, const in const_df[['id', 'abbreviation']].iterrows():
                        if const['id'] not in anchortable_con['id'].values:
                            anchortable_con = pd.concat([anchortable_con, pd.DataFrame([const])], ignore_index=True)
                            postgresdb.insert_dataframe(anchortable_con, table_name='constructors')
                            postgresdb.RunQuery('ALTER TABLE constructors ADD CONSTRAINT pk_constructors_id PRIMARY KEY(id);')


                    # ---------------- DRIVER RACE RESULTS ----------------
                    driver_race_data, key = jsontodf.DriverRaceResults(i)
                    driverdf_race = jsontodf.loadtodf(driver_race_data)
                    driverdf_race['team'] = driverdf_race['id'].str.split('_', expand=True)[0]
                    driverdf_race = PointTextColumnDeletion(driverdf_race)
                    driver_race_table = generate_table_name(key, i, raw_data)
                    logging.info("Driver race results processed. Table: %s", driver_race_table)

                    # ---------------- DRIVER QUALIFYING RESULTS ----------------
                    driver_quali_data, key = jsontodf.DriverQualifyingResults(i)
                    driverquali_df = jsontodf.loadtodf(driver_quali_data)
                    driverquali_df = PointTextColumnDeletion(driverquali_df)
                    driverquali_df['team'] = driverquali_df['id'].str.split('_', expand=True)[0]
                    driver_quali_table = generate_table_name(key, i, raw_data)
                    logging.info("Driver qualifying results processed. Table: %s", driver_quali_table)

                    # ---------------- CONSTRUCTOR RACE RESULTS ----------------
                    consructor_race_data, key = jsontodf.Constructor_race_results(i)
                    consructordf_race = jsontodf.loadtodf(consructor_race_data)
                    consructordf_race = PointTextColumnDeletion(consructordf_race)
                    consructor_race_table = generate_table_name(key, i, raw_data)
                    logging.info("Constructor race results processed. Table: %s", consructor_race_table)

                    # ---------------- CONSTRUCTOR QUALIFYING RESULTS ----------------
                    consructor_quali_data, key = jsontodf.Constructor_qualifying_results(i)
                    consructordf_quali = jsontodf.loadtodf(consructor_quali_data)
                    consructordf_quali = PointTextColumnDeletion(consructordf_quali)
                    consructor_quali_table = generate_table_name(key, i, raw_data)
                    logging.info("Constructor qualifying results processed. Table: %s", consructor_quali_table)

                    # ---------------- SPRINT EVENTS ----------------
                    if i in sprint_rounds:
                        logging.info("Sprint round detected for round %d — processing sprint data...", i)
                        driver_sprint_data, key = jsontodf.DriverSprintResults(i)
                        driversprint_df = jsontodf.loadtodf(driver_sprint_data)
                        driversprint_df = PointTextColumnDeletion(driversprint_df)
                        driversprint_df['team'] = driversprint_df['id'].str.split('_', expand=True)[0]
                        driver_sprint_table = generate_table_name(key, i, raw_data)

                        consructor_sprint_data, key = jsontodf.Constructor_sprint_results(i)
                        consructordf_sprint = jsontodf.loadtodf(consructor_sprint_data)
                        consructordf_sprint = PointTextColumnDeletion(consructordf_sprint)
                        consructor_sprint_table = generate_table_name(key, i, raw_data)
                        logging.info("Sprint data processed for round %d", i)
                    else:
                        driversprint_df = consructordf_sprint = driver_sprint_table = consructor_sprint_table = None

                    # ---------------- LOAD TO DATABASE ------------------------
                    try:
                        #logging.info("⬆️ Starting data load for round %d...", i)
                        #postgresdb.insert_dataframe(trackdf, table_name=track_table)
                        postgresdb.insert_dataframe(driverdf, table_name=driver_table)
                        postgresdb.RunQuery(f'ALTER TABLE {driver_table} ADD CONSTRAINT fk_drivers_id FOREIGN KEY(id) REFERENCES drivers(id);')
                        postgresdb.insert_dataframe(const_df, table_name=consructor_table)
                        postgresdb.RunQuery(f'ALTER TABLE {consructor_table} ADD CONSTRAINT fk_constructors_id FOREIGN KEY(id) REFERENCES constructors;')
                        postgresdb.insert_dataframe(driverdf_race, table_name=driver_race_table)
                        postgresdb.RunQuery(f'ALTER TABLE {driver_race_table} ADD CONSTRAINT fk_drivers_id FOREIGN KEY(id) REFERENCES drivers(id);')
                        postgresdb.insert_dataframe(driverquali_df, table_name=driver_quali_table)
                        postgresdb.RunQuery(f'ALTER TABLE {driver_quali_table} ADD CONSTRAINT fk_drivers_id FOREIGN KEY(id) REFERENCES drivers(id);')
                        postgresdb.insert_dataframe(consructordf_race, table_name=consructor_race_table)
                        postgresdb.RunQuery(f'ALTER TABLE {consructor_race_table} ADD CONSTRAINT fk_constructors_id FOREIGN KEY(id) REFERENCES constructors;')
                        postgresdb.insert_dataframe(consructordf_quali, table_name=consructor_quali_table)
                        postgresdb.RunQuery(f'ALTER TABLE {consructor_quali_table} ADD CONSTRAINT fk_constructors_id FOREIGN KEY(id) REFERENCES constructors;')

                        if driversprint_df is not None:
                            postgresdb.insert_dataframe(driversprint_df, table_name=driver_sprint_table)
                            postgresdb.RunQuery(f'ALTER TABLE {driver_sprint_table} ADD CONSTRAINT fk_drivers_id FOREIGN KEY(id) REFERENCES drivers(id);')
                            postgresdb.insert_dataframe(consructordf_sprint, table_name=consructor_sprint_table)
                            postgresdb.RunQuery(f'ALTER TABLE {consructor_sprint_table} ADD CONSTRAINT fk_constructors_id FOREIGN KEY(id) REFERENCES constructors;')


                        logging.info("✅ All data successfully inserted for round %d.", i)

                    except Exception as e:
                        logging.error("❌ Database insertion failed for round %d: %s", i, e)
                        logging.debug(traceback.format_exc())


                except Exception as e:
                    logging.error("❌ Error during processing of round %d: %s", i, e)
                    logging.debug(traceback.format_exc())
                    continue  # Continue to next round even if this one fails

                postgresdb.insert_dataframe(trackdf, table_name=track_table)
                postgresdb.RunQuery(f'ALTER TABLE {track_table} ADD CONSTRAINT pk_track_id PRIMARY KEY(id);')

            logging.info("🎯 Transformation and loading process completed successfully for all rounds.")

        except Exception as e:
            logging.error("💥 ETL transformation step failed: %s", e)
            logging.debug(traceback.format_exc())

    except Exception as e:
        logging.error("💥 Top-level ETL failure: %s", e)
        logging.debug(traceback.format_exc())


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


    for api in apis:
        success = RunEtlPipeline(api)
        if not success:
            logging.error(f"Stopping ETL process due to failure in {api}.")
            return  # stop the function instead of using 'break'
        time.sleep(2)  # optional pause between API runs

    logging.info("All ETL jobs complete.")



if __name__ == "__main__":
    main()
