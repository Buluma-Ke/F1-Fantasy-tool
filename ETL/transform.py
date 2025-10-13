import pandas as pd
import json

class F1FantasyFrameBuilder(object):

    def __init__(self, jsondata):

        self.jsondata = jsondata
        #self.year = year

    def loadjson(self):
        """
        Args  : JSON file location; file path
                including nested jason
        return: Dictionery of the a f1 seasons race results as data
        """
        try:
            # ✅ check if the data came from extract.py
            if isinstance(self.jsondata, dict):
                print("Detected API response data from extract.")
                self.data = self.jsondata
            else:
                # ass assume its a file path
                print(f"Loading JSON data from file: {self.jsondata}")
                with open(self.jsondata, "r", encoding="utf-8") as f:
                    self.data = json.load(f)

            # ✅ Safely access nested JSON keys
            season_result = self.data.get("seasonResult", {})
            race_results = season_result.get("raceResults", [])
            season = season_result.get("season", "Unknown")


            print(f"{len(race_results)} races found throughout the {season} F1 season")
            print("JSON data loaded successfully")

                # print(f"{len(self.data['seasonResult']['raceResults'])} races found throughout the {self.data['seasonResult']["season"]} F1 season")
                # print("JSON file loaded succesfully")
                # print(self.data['seasonResult']["raceResults"])
            return self.data


        except FileNotFoundError:
            print(f"Error: File '{self.jsondata}' not found.")
        except json.JSONDecodeError:
            print("Error: Invalid JSON format.")
        except Exception as e:
            print(f"Unexpected error while loading JSON: {e}")

        return None




    def track_data(self):

        """
        Args   : Dictionery object stored in self.data["races"]
        returns: List of dictioneries containing track information
        """

        track_list = []
        key = 'race'
        self.track_info = self.data.get('races', {})
        #table_key = list(self.track_info.keys())[0]
       # print(table_key)
        print(f'{len(self.track_info)} tracks found') # should be 24

        for track in self.track_info:
            track_dict = {}
            for key, value in track.items():
                if key == 'flagUrl' or  key == 'circuitMapUrl':
                    continue

                track_dict[key] = value
                #print(track_dict)
            #print(f'{track_dict['name']} added')

            track_list.append(track_dict)

        print(f'{len(track_list)} tracks added!')
        print(track_list[0]['start_times'])
        return track_list, key


    def raceresults(self):
        """
        Args   : Dictionery object stored in self.data["raceResults]
        Returns: a list of dictioneries of each round results
        """
        self.results = self.data['seasonResult']["raceResults"]

        return self.results



    def DriverResults(self, rounNUmber):
        """
        Args   : Dictionary object from raceresults(self) stored in results:
                 roundNumber; the race event
        Returns: A list of how each driver faired in the event
        """
        self.driver_results = self.results[f'{rounNUmber}']['drivers']
        DriverResult_list = []
        key = 'driver_result'

        for i in range(len(self.driver_results)):
            driver_dict = {}
            for k, v in self.driver_results[i].items():
                #print( k == 'raceResult')
                if k == 'raceResult':
                    #print(f"Skipping raceResult at index {i}")

                    continue

                driver_dict[k] = v
            DriverResult_list.append(driver_dict)
            # for i in range(len(self.driver_results)):
            #     driver_dict = {k: v for k, v in self.driver_results[i].items() if k != 'raceResult'}
            #     DriverResult_list.append(driver_dict)

            #print(f"{i} {driver_dict['id']} loaded")


        #clsprint(drivers_list)
        #print(f"{len(DriverResult_list)} drivers loaded")

        return DriverResult_list, key



    def CustructorsResults(self, roundNumber):
        """
        Args   : Dictionary object from raceresults(self) stored in results:
                 roundNumber; the race event
        Returns: A list of how each constructor faired in the event
        """
        self.custructorsresults = self.results[f"{roundNumber}"]['constructors']
        constructorResult_list = []
        key = 'constructor_result'

        for i in range(len(self.custructorsresults)):
            constructor_dict = {}
            for key, value in self.custructorsresults[i].items():
                if key == "raceResult":
                    continue
                constructor_dict[key] = value
            constructorResult_list.append(constructor_dict)
            print(f"{i} {constructor_dict['id']} loaded")

        return constructorResult_list, key


    def _extract_results(self, roundNumber, category, session_type):
        """
        Generic helper to extract results for drivers or constructors.

        Args   : roundNumber (int rep as str): The race round number.
                 category (str): 'drivers' or 'constructors'.
                 session_type (str): 'R' for race, 'Q' for qualifying.

        Returns: list[dict]: List of result dictionaries.
        """
        data = self.results.get(f'{roundNumber}', {}).get(category, [])
        result_list = []

        for item in data:
            race_result = item.get('raceResult', {})
            session_results = race_result.get(session_type)

            # Skip if this session doesn't exist for this round
            if not session_results:
                continue

            entity_id = item.get('id', 'unknown')
            entity_dict = {"id": entity_id}

            for key, values in session_results.items():
                for subkey, val in values.items():
                    entity_dict[f"{session_type}_{key}_{subkey}"] = val

            result_list.append(entity_dict)

        print(f"{category.capitalize()} {session_type} results found: {len(result_list)}")
        return result_list



    def DriverRaceResults(self, roundNumber):
        key = 'driver_race_result'
        return self._extract_results(roundNumber, 'drivers', 'R'), key


    def DriverQualifyingResults(self, roundNumber):
        key = 'driver_qualifying_result'
        return self._extract_results(roundNumber, 'drivers', 'Q'), key


    def DriverSprintResults(self, roundNumber):
        key = 'driver_sprint_result'
        return self._extract_results(roundNumber, 'drivers', 'S'), key


    def Constructor_race_results(self, roundNumber):
        key = 'contructor_race_result'
        return self._extract_results(roundNumber, 'constructors', 'R'), key


    def Constructor_qualifying_results(self, roundNumber):
        key = 'custrutcor_qualifying_result'
        return self._extract_results(roundNumber, 'constructors', 'Q'), key


    def Constructor_sprint_results(self, roundNumber):
        key = 'constructor_sprint_result'
        return self._extract_results(roundNumber, 'constructors', 'S'), key


    def loadtodf(self, data_list):
        """
        input  : list object i.e stored in drivers_list
        Returns: Dataframe object
        """
        df = pd.DataFrame(data_list)
        return df
        # import pandas as pd





#Example usage
