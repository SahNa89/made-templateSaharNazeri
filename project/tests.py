import pandas as pd
import os
import unittest
#from unittest.mock import patch
import importlib
pipelinemain = importlib.import_module("pipeline-main")
import sqlite3
import pathlib

#testing each part of a pipeline in isolation
class TestPipelinecomponents(unittest.TestCase):

    def setUp(self):
        year= 2021
        self.year = year
        self.sampleBCurl = "https://cidportal.jrc.ec.europa.eu/ftp/jrc-opendata/ABCIS/AtmosphericParticles/Ver{year}-01-01/Equiv_BlackCarbon_AETH_{year}.csv"
        self.sampleDMPSurl = "https://cidportal.jrc.ec.europa.eu/ftp/jrc-opendata/ABCIS/AtmosphericParticles/Ver{year}-01-01/DMPS_Particle_Concentration_{year}.csv"
        self.sampleweatherurl ="http://bulk.meteostat.net/v2/daily/16066.csv.gz"

        self.db_name = "AtmosphericAndTemperatureAnalytics"
        self.table_name = ["DMPS Table", "BlackCarbon Table","WeatherTB"]
        cwd = pathlib.Path(__file__).parent.parent.parent.parent.resolve()
        self.db_path = f"{cwd}/data/{self.db_name}.sqlite"

    def test_extraction_csv(self):
        extractionData = pipelinemain.extractionData()
        self.assertEqual(extractionData, "csv")
        dtest = pipelinemain.Data()
        #loadData( ["DMPS Table", "BlackCarbon Table","WeatherTB"]))
        ##test1=pipelinemain.main()
        dtest.loadData( ["DMPS Table", "BlackCarbon Table","WeatherTB"])
        x= dir(pipelinemain)
        print(x)
        relative_path = "../data/AtmosphericAndTemperatureAnalytics.sqlite"
        db_path = os.path.abspath(relative_path)
        print(db_path)
        conn = sqlite3.connect(db_path)
        weather_data_df = pd.read_sql_query(f"SELECT * FROM [WeatherTB]", conn)
        weather_data_df.head()

    from pathlib import Path

    THIS_DIR = Path(__file__).parent

    my_data_path = THIS_DIR.parent / 'data_folder/data.csv'

    # or if it's in the same directory
    my_data_path = THIS_DIR / 'testdata.csv'
    def test_to_extension_invalid_format(self):
        pass
    def test_load_mock(self):
        data = pd.DataFrame([[1, 2, 31], [3, 4, 511], columns = ('a'))
        with unittest.mock.patch.object(data, "to_sql") as to_sql_mock:
            Load(data, "test_load‚sqlite")
            to_sql_mock.assert_called_once()
#integration and system-test level
        with self.assertEqual():

class TestSystemLevel(unittest.TestCase):
    def setUp(self):
        self.db_name = "AtmosphericAndTemperatureAnalytics"
        self.table_name = ["DMPS Table", "BlackCarbon Table", "WeatherTB"]
        cwd = pathlib.Path(__file__).parent.parent.parent.parent.resolve()
        self.db_path = f"{cwd}/data/{self.db_name}.sqlite"


if __name__ == "__main__":
    unittest.main()
#real = ProductionClass()
#real.something = MagicMock()
#real.method()
#real.something.assert_called_once_with(1, 2, 3)

