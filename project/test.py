import pandas as pd

DurationScope = [2023, 2022, 2021, 2020, 2019]
headers = ['Date', 'tavg', 'tmin', 'tmax', 'precipitation', 'snow', 'wdir', 'wspd', ' peak_wind',
                           'air_pressure', 'tsun']
df = pd.read_csv("http://bulk.meteostat.net/v2/daily/16066.csv.gz" , compression='gzip')
df.columns = headers

print(' type df\n',type(df['Date']))
print('the first df\n',df)
df.drop(['snow', 'wdir', 'wspd', ' peak_wind','air_pressure', 'tsun'], axis='columns', inplace=True)
#df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d', errors='coerce')
print(' type2 df\n',type(df['Date']))
new_WTF = df.query("Date >= '"+str(DurationScope[-1])+"-01-01' and Date < '"+str(DurationScope[0]+1)+"-01-01'")
#df.loc[df['Date'].dt.year >= 2019 and df['Date'].dt.year <= 2023]

print('the last df\n',new_WTF)


#----------------------data report

#.filter(max()) #perday I have a max pollution

print('cc ',new_WTF.columns)
print('cc ',new_WTF.dtypes)

print('cc ',new_WTF.head(10))

print('cc ',new_WTF.describe())
