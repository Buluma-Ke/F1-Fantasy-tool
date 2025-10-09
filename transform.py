from ETL.transform import F1FantasyFrameBuilder
import pandas as pd


jsontodf = F1FantasyFrameBuilder('C:/Users/MWITA/Desktop/F1DB/Data/f1fantasydata2024.json')
jsonfile = jsontodf.loadjson()
trackdata = jsontodf.track_data()
trackdf = jsontodf.loadtodf(trackdata)
raceresults = jsontodf.raceresults()
# race_1_Driver_results = jsontodf.DriverResults(1)
# race_1_Driver_results_df = jsontodf.loadtodf(race_1_Driver_results)
race_1_constructor_results = jsontodf.CustructorsResults(1)
race_1_constructor_results_df = jsontodf.loadtodf(race_1_constructor_results)
# race_1_Driver_race_results = jsontodf.DriverRaceResults(1)
# race_1_Driver_race_results_df = jsontodf.loadtodf(race_1_Driver_race_results)
# race_1_constructor_race_results = jsontodf.Constructor_race_results(1)
# race_1_constructor_race_results_df = jsontodf.loadtodf(race_1_constructor_race_results)
# race_1_Driver_quali_results = jsontodf.DriverQualifyingResults(1)
# race_1_Driver_quali_results_df = jsontodf.loadtodf(race_1_Driver_quali_results)
# race_1_constructor_quali_results = jsontodf.Constructor_qualifying_results(1)
# race_1_constructor_quali_results_df = jsontodf.loadtodf(race_1_constructor_quali_results)
