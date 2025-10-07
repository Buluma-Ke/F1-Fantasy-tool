import pandas as pd
import json

class JsonToDfStep(object):

    def __init__(self, jasonfile):

        self.jsonfile = jasonfile


    def loadjson(self):
        """
        input: JSON file location; file path
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
        input: Dictionery object stored in self.data["races"]
        returns: List pf dictioneries containing track information
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







    def driver_data(self):
        """
        Input: Dictionary object stored in self.data["raceResults"]
        Returns: A list of drivers throughot the season dictionaries (each corresponding to a driver)
        """
        self.driver_data = self.data["raceResults"]
        drivers_list = []
        #drivers = self.data[f'1']['drivers'] # list
        for round in range(2, len(self.driver_data)):                  # Works, dont as me how
            drivers = self.driver_data[f'{round}']['drivers'] # list
            for driver in drivers:
                if driver in drivers:
                    pass
                else:
                    print("Extra driver found")

        for i in range(len(drivers)):
            driver_dict = {}
            for key, value in drivers[i].items():
                if key == 'raceResult':
                    break
                driver_dict[key] = value
            drivers_list.append(driver_dict)
            print(f"{i} {driver_dict['id']} loaded")


        #clsprint(drivers_list)
        print(f"{len(drivers_list)} drivers loaded")

        return drivers_list



    #def constractor_data(self):



    def loadtodf(self, data_list):
        """
        input: list object i.e stored in drivers_list
        Returns: Dataframe object
        """
        df = pd.DataFrame(data_list)
        print(df)
        return df
        # import pandas as pd





#Example usage

jsonfile = JsonToDfStep("f1fantasydata2024.json")
data = jsonfile.loadjson()
# listof_drivers = jsonfile.driver_data()
# driver_df = jsonfile.loadtodf(listof_drivers)
track_data = jsonfile.track_data()
track_data = jsonfile.loadtodf(track_data)
