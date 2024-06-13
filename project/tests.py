import pandas as pd
import os
import unittest
from unittest.mock import patch
import importlib
pipelinemain = importlib.import_module("pipeline-main")
import sqlite3
import pathlib

#testing each part of a pipeline in isolation
class TestPipelinecomponents(unittest.TestCase):

    sampleBCurl = "https://cidportal.jrc.ec.europa.eu/ftp/jrc-opendata/ABCIS/AtmosphericParticles/Ver2021-01-01/Equiv_BlackCarbon_AETH_2021.csv"
    sampleDMPSurl = "https://cidportal.jrc.ec.europa.eu/ftp/jrc-opendata/ABCIS/AtmosphericParticles/Ver2021-01-01/DMPS_Particle_Concentration_2021.csv"
    sampleweatherurl = "http://bulk.meteostat.net/v2/daily/16066.csv.gz"
    durationScope = [ 2021]
    ExpectedColumBC = (['Date', 'Black Carbon'])
    ExpectedColumDMPS = ['Date', 'DMPS_Total']
    ExpectedColumweather = ['Date', 'tavg', 'tmin', 'tmax']
    dir = pathlib.Path(__file__).parent.parent
    #print("this is directory", dir)
    dbname = "AtmosphericAndTemperatureAnalytics"
    dbpath = f"{dir}/data/{dbname}.sqlite"
    # print("this is path", self.dbpath)
    #print("this is path", dbpath)
    #Validates that the output file(s) exist (but make sure the data pipeline creates them and you do not check them in)
    @classmethod
    def setUpClass(cls):
        if os.path.exists(cls.dbpath):
            os.remove(cls.dbpath)

    @classmethod
    def tearDownClass(cls) -> None:
        if os.path.exists(cls.dbpath):
            os.remove(cls.dbpath)

    def test_extractionData(self):
        #print(self.sampleBCurl)
        Test = pipelinemain.Data.extractionData(self, [self.sampleBCurl])
        TestDMPS = Test[0]
        TestBc = Test[1]
        Testweather = pipelinemain.Data.extractionData(self, [self.sampleweatherurl])
        #print(TestBc.dtypes)
        #ExpectedInput =pd.DataFrame()
        #pd.testing.assert_frame_equal(TestBc ,ExpectedInput)

        self.assertFalse(TestBc.empty, "Downloaded Black Carbon data should not be empty")
        self.assertFalse(TestDMPS.empty, "Downloaded DMPS data should not be empty")
        self.assertFalse(Testweather.empty, "Downloaded weather data should not be empty")
        self.assertEqual(len(TestBc.columns), 2)
        self.assertEqual(len(TestDMPS.columns), 3)
        self.assertEqual(len(Testweather.columns), 11)
    def test_transformData(self):
        Testtransform =  pipelinemain.Data.extractionData(self, [self.sampleBCurl])
        TesttransformDMPS = pipelinemain.Data.transformData(self,Testtransform[0],self.sampleDMPSurl)
        TesttransformBc = pipelinemain.Data.transformData(self,Testtransform[1],self.sampleDMPSurl)
        Testtransformweather = pipelinemain.Data.transformData(self, pipelinemain.Data.extractionData(self, [self.sampleweatherurl]), self.sampleweatherurl)
        self.assertCountEqual(TesttransformBc.columns, self.ExpectedColumBC,
                              msg="Columns have not been transformed correctly.")
        self.assertCountEqual(TesttransformDMPS.columns, self.ExpectedColumDMPS,
                              msg="Columns have not been transformed correctly.")
        self.assertCountEqual(Testtransformweather.columns, self.ExpectedColumweather,
                              msg="Columns have not been transformed correctly.")

        self.assertFalse(TesttransformBc.isna().sum().sum(), "There is Nan Value in TesttransformBc")
        self.assertFalse(TesttransformDMPS.isna().sum().sum(), "There is Nan Value in TesttransformDMPS")
        self.assertFalse(Testtransformweather.isna().sum().sum(), "There is Nan Value in Testtransformweather")

        self.assertEqual(len(TesttransformBc), 50124 , "Row number mismatch")
        self.assertEqual(len(TesttransformDMPS), 45836 , "Row number mismatch")
        self.assertEqual(len(Testtransformweather), 365 , "Row number mismatch")

        self.assertListEqual(list(TesttransformBc.columns), self.ExpectedColumBC,
                              msg="Column's names have not been transformed correctly.")
        self.assertListEqual(list(TesttransformDMPS.columns), self.ExpectedColumDMPS,
                              msg="Column's names have not been transformed correctly.")
        self.assertListEqual(list(Testtransformweather.columns), self.ExpectedColumweather,
                              msg="Column's names have not been transformed correctly.")


  #####-----------testing ETL pipeline System level as a whole---------------------------------------------
class TestSystemLevel(unittest.TestCase):
    def setUp(self):
        self.durationScope = [2021]
        dir = pathlib.Path(__file__).parent.parent
        #print("this is directory",dir)
        self.dbname = "AtmosphericAndTemperatureAnalytics"
        self.tablename = ["DMPS Table", "BlackCarbon Table", "WeatherTB"]
        self.dbpath = f"{dir}/data/{self.dbname}.sqlite"
        #print("this is path", self.dbpath)
    def tearDown(self):
        if os.path.exists(self.dbpath):
            os.remove(self.dbpath)

    def test_loadData(self , *args):
         # .loadData(self, self.tablename)
        with patch.object (pipelinemain.Data , "loadData") as mock:
            mock.loadData(self, self.tablename)
            mock.loadData.assert_called_with(self, self.tablename)


        pipelinemain.Data.loadData(self, self.tablename)
        self.assertTrue(os.path.exists(self.dbpath))
        conn = sqlite3.connect(self.dbpath)
        newtablecountBC = pd.read_sql_query("SELECT count(*) FROM [BlackCarbon Table]", conn)
        newtablecountDMPS = pd.read_sql_query("SELECT count(*) FROM [DMPS Table]", conn)
        newtablecountWeatherTB = pd.read_sql_query("SELECT count(*) FROM [WeatherTB]", conn)
        #print(newtablecountDMPS)
        self.assertEqual(45836, newtablecountDMPS.values, "Row number mismatch")
        self.assertEqual(50124, newtablecountBC.values, "Row number mismatch")
        self.assertEqual(365, newtablecountWeatherTB .values, "Row number mismatch")
        conn.commit()
        conn.close()




if __name__ == "__main__":
    unittest.main()


