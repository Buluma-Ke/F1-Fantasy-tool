import pandas as pd
import json

class F1FantasyFrameBuilder(object):

    def __init__(self, jasonfile):

        self.jsonfile = jasonfile


    def loadjson(self):
        """
        Args  : JSON file location; file path
                including nested jason
        return: Dictionery of the a f1 seasons race results as data
        """
        try:
            with open(self.jsonfile) as f:
                self.jsonloaded = json.load(f)
                self.data = self.jsonloaded # since the data only contains the 2024 f1 seasons
                print("JSON file loaded succesfully")
                print(f"{len(self.data['seasonResult']['raceResults'])} races found throughout the {self.jsonloaded['seasonResult']["season"]} F1 season")

                return self.data

        except:
            print("Error loading JSON file")




    def track_data(self):

        """
        Args   : Dictionery object stored in self.data["races"]
        returns: List of dictioneries containing track information
        """

        track_list = []
        self.track_info = self.data['races']
        print(f'{len(self.track_info)} tracks found') # should be 24

        for track in self.track_info:
            track_dict = {}
            for key, value in track.items():
                if key == 'flagUrl' or  key == 'circuitMapUrl':
                    continue

                track_dict[key] = value
                #print(track_dict)
            print(f'{track_dict['name']} added')

            track_list.append(track_dict)

        print(f'{len(track_list)} tracks added!')
        return track_list


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

        for i in range(len(self.driver_results)):
            driver_dict = {}
            for key, value in self.driver_results[i].items():
                if key == 'raceResult':
                    pass
                driver_dict[key] = value
            DriverResult_list.append(driver_dict)
            print(f"{i} {driver_dict['id']} loaded")


        #clsprint(drivers_list)
        print(f"{len(DriverResult_list)} drivers loaded")

        return DriverResult_list



    def CustructorsResults(self, roundNumber):
        """
        Args   : Dictionary object from raceresults(self) stored in results:
                 roundNumber; the race event
        Returns: A list of how each constructor faired in the event
        """
        self.custructorsresults = self.results[f"{roundNumber}"]['constructors']
        constructorResult_list = []

        for i in range(len(self.custructorsresults)):
            constructor_dict = {}
            for key, value in self.custructorsresults[i].items():
                if key == "raceResult":
                    continue
                constructor_dict[key] = value
            constructorResult_list.append(constructor_dict)
            print(f"{i} {constructor_dict['id']} loaded")

        return constructorResult_list


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
        return self._extract_results(roundNumber, 'drivers', 'R')


    def DriverQualifyingResults(self, roundNumber):
        return self._extract_results(roundNumber, 'drivers', 'Q')


    def DriverSprintResults(self, roundNumber):
        return self._extract_results(roundNumber, 'drivers', 'S')


    def Constructor_race_results(self, roundNumber):
        return self._extract_results(roundNumber, 'constructors', 'R')


    def Constructor_qualifying_results(self, roundNumber):
        return self._extract_results(roundNumber, 'constructors', 'Q')


    def Constructor_sprint_results(self, roundNumber):
        return self._extract_results(roundNumber, 'constructors', 'S')


    def loadtodf(self, data_list):
        """
        input  : list object i.e stored in drivers_list
        Returns: Dataframe object
        """
        df = pd.DataFrame(data_list)
        print(df)
        return df
        # import pandas as pd





#Example usage
