import pandas as pd
import os
import sqlite3
     
class Data:
    def __init__(self):
        Localpath = "../data"
        os.makedirs(Localpath, exist_ok=True, mode=0o777)
        self.durationScope = [2022, 2021, 2020, 2019,2018]

    ## ETL for Weather, DMPS and BlackCarbon datasets
    # ExtractionData : Download by the URL and extracted data source
    def extractionData(self,dataset_urls):
        if dataset_urls[0].endswith('.gz'):
            #print('the second list df\n', dataset_urls[0])
            df = pd.read_csv(dataset_urls[0], compression='gzip')
            #print('the last df\n', df)
            return df

        if dataset_urls[0].endswith('.csv'):
            df0 = pd.DataFrame()
            df1 = pd.DataFrame()
            for year in self.durationScope:
                DMPS_ParticleYear = f"https://cidportal.jrc.ec.europa.eu/ftp/jrc-opendata/ABCIS/AtmosphericParticles/Ver{year}-01-01/DMPS_Particle_Concentration_{year}.csv"
                BlackCarbonYear = f"https://cidportal.jrc.ec.europa.eu/ftp/jrc-opendata/ABCIS/AtmosphericParticles/Ver{year}-01-01/Equiv_BlackCarbon_AETH_{year}.csv"
                #print(year, DMPS_ParticleYear, BlackCarbonYear)
                dataset_urls=[DMPS_ParticleYear, BlackCarbonYear]
                df00 = pd.read_csv(dataset_urls[0])
                df0 = pd.concat([df00,df0], ignore_index=True)
                df11 = pd.read_csv(dataset_urls[1])
                df1 = pd.concat([df11,df1], ignore_index=True)
                #print(' list df\n',df0,df1)
            return df0,df1
    #TransformData
    def transformData(self,TF,DatasetUrl):
        # TransformData for DMPS and BlackCarbon datasets
        #print('count before ',len(TF))
        TF.drop_duplicates(inplace=True)  #check and remove duplicated rows
        # check validation
        #print('is null count value ',TF.isna().sum())  #Check null values
        #print('name of columns ',TF.columns)
        #print('type of columns',TF.dtypes)
        #TF.head(10)
        #TF.describe()
        #print('count after ',len(TF))
        # remove column that most of them are nan and it is not usefull
        if "CPC_Total" in TF.columns:
            TF.drop("CPC_Total", axis='columns',inplace=True)
        if "DateTime" or "Start DateTime" in TF.columns:
            TF.rename(columns={'DateTime':'Date' , 'Start DateTime':'Date'},inplace=True)
        if "Date"  in TF.columns:
            TF["Date"] = pd.to_datetime(TF["Date"])
        # Convert to [int] values as it is
        if "BC"  in TF.columns:
            TF["BC"] = TF["BC"].astype(int)
            TF.rename(columns={"BC": "Black Carbon"}, inplace=True)
        #print('the last df\n',TF)
        # TransformData for Weather Dataset
        if DatasetUrl.endswith('.gz'):
            WDatasetHeaders = ['Date', 'tavg', 'tmin', 'tmax', 'precipitation', 'snow', 'wdir', 'wspd', ' peak_wind',
                           'air_pressure', 'tsun']
        #Add Header for Weather data frame of TF
            TF.columns = WDatasetHeaders
        #remove useless columns
            TF.drop(['precipitation','snow', 'wdir', 'wspd', ' peak_wind', 'air_pressure', 'tsun'], axis='columns', inplace=True)
            #print(TF)
            if "Date" in TF.columns:
                TF["Date"] = pd.to_datetime(TF["Date"])
        #scope duration filter and clean data
            filtered_TF = TF.query(
            "Date >= '" + str(self.durationScope[-1]) + "-01-01' and Date < '" + str(self.durationScope[0] + 1) + "-01-01'")
            #print(filtered_TF )
            TF = filtered_TF
        return TF
    # LoadData
    def loadData(self,table_names):
        DMPS_ParticleYear1 = f"https://cidportal.jrc.ec.europa.eu/ftp/....csv"
        BlackCarbonYear1 = f"https://cidportal.jrc.ec.europa.eu/ftp/....csv"
        WeatherUrl = f"http://bulk.meteostat.net/v2/daily/16066.csv.gz"  # Milano / Malpensa
        ExtractData = Data.extractionData(self,[DMPS_ParticleYear1, BlackCarbonYear1])
        #print('the first list df\n',ExtractData)
        df1 = Data.transformData(self,ExtractData[0],DMPS_ParticleYear1)
        df2 = Data.transformData(self,ExtractData[1],BlackCarbonYear1)

        # s = urllib.request.get(dataset_urls).content.decode("utf8")
        ExtractDataWeather = Data.extractionData(self,[WeatherUrl])
        #print('the ExtractDataWeather  df\n', ExtractDataWeather)
        df3 = Data.transformData(self,ExtractDataWeather,WeatherUrl)
        os.makedirs(os.path.dirname("../data/AtmosphericAndTemperatureAnalytics.sqlite"), exist_ok=True)
        conn = sqlite3.connect("../data/AtmosphericAndTemperatureAnalytics.sqlite")
        df1.to_sql(table_names[0], conn, if_exists="replace", index=False)
        df2.to_sql(table_names[1], conn, if_exists="replace", index=False)
        df3.to_sql(table_names[2], conn, if_exists="replace", index=False)
        conn.close()


if __name__ == "__main__":
    d= Data()
    d.loadData(["DMPS Table", "BlackCarbon Table","WeatherTB"])

