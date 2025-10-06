import pandas as pd
import json

class JsonToDfStep(object):

    def __init__(self, jasonfile):
        """
        accepts a jason file
        including nested jason
        """
        self.jsonfile = jasonfile


    def loadjson(self):
        """
        Load Json file
        returns a dictionery of the 2024 f1 seasons race results as data
        """
        with open(self.jsonfile) as f:
            self.data = json.load(f)
            self.data = self.data['seasonResult']['raceResults'] # since the data only contains the 2024 f1 seasons

            return self.data

    def driver_data(self):

        self.driver_dict = {}
        drivers = self.data['1']['drivers'] # list

        for i in range(0:21):
            for key, value in drivers[i].items():
                    if key == 'raceResult':
                        break
                        gas_dict[key] = value
