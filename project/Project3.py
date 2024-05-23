import pandas as pd
import os
import sqlite3

dataset_url1 = "https://cidportal.jrc.ec.europa.eu/ftp/jrc-opendata/ABCIS/AtmosphericParticles/Ver2021-01-01/DMPS_Particle_Concentration_2021.csv"
dataset_url2 = "https://cidportal.jrc.ec.europa.eu/ftp/jrc-opendata/ABCIS/AtmosphericParticles/Ver2021-01-01/Equiv_BlackCarbon_AETH_2021.csv"
Localpath = "../data"
os.makedirs(Localpath, exist_ok=True,mode= 0o777)


def pipeline(urls, db_name, table_names):
    df1 = pd.read_csv(urls[0])
    df2 = pd.read_csv(urls[1])
    db_path = os.path.join(Localpath, db_name)
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    df1.to_sql(table_names[0], conn, if_exists="replace", index=False)
    df2.to_sql(table_names[1], conn, if_exists="replace", index=False)

    conn.close()

    pipeline([dataset_url1,dataset_url2], "Atmospheric.sqlite", ["DMPS", "BlackCarbon"])
