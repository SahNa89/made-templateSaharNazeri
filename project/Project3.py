import pandas as pd
import os
import sqlite3
#import urllib.request

dataset_url1 = "https://cidportal.jrc.ec.europa.eu/ftp/jrc-opendata/ABCIS/AtmosphericParticles/Ver2021-01-01/DMPS_Particle_Concentration_2021.csv"
dataset_url2 = "https://cidportal.jrc.ec.europa.eu/ftp/jrc-opendata/ABCIS/AtmosphericParticles/Ver2021-01-01/Equiv_BlackCarbon_AETH_2021.csv"
Localpath = "../data"
os.makedirs(Localpath, exist_ok=True,mode= 0o777)


def CreateData(dataset_urls, table_names):
    #s = urllib.request.get(dataset_urls).content.decode("utf8")
    df1 = pd.read_csv(dataset_urls[0])
    df2 = pd.read_csv(dataset_urls[1])
    os.makedirs(os.path.dirname("../data/Atmospheric.sqlite"), exist_ok=True)
    conn = sqlite3.connect("../data/Atmospheric.sqlite")
    df1.to_sql(table_names[0], conn, if_exists="replace", index=False)
    df2.to_sql(table_names[1], conn, if_exists="replace", index=False)

    conn.close()

CreateData([dataset_url1,dataset_url2], ["DMPS", "BlackCarbon"])
