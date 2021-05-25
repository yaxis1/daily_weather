from netCDF4 import Dataset
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import psycopg2 
import io
import time

start_time = time.time()

data = Dataset('/mnt/data/rasters/WindData2020_03_13_to_26.nc')

#print(data.variables.keys())
#dict_keys(['longitude', 'latitude', 'time', 'u10', 'v10'])

#Accessing data 

time_data = data.variables['time'][:]
longitude_data = data.variables['longitude'][:]
latitude_data = data.variables['latitude'][:]

#u10_data = data.variables['u10'][:] #text
#v10_data = data.variables['v10'][:] #text


def check_scale(x):
    if hasattr(data.variables[str(x)],'scale_factor'):
        return (data.variables[str(x)].scale_factor)
    else: 
        return ('none')

df_meta_data = pd.DataFrame(columns=['Long Name','Units'])

for x in data.variables.keys():
    df_meta_data = df_meta_data.append({'Long Name' : data.variables[str(x)].long_name, 
                                        'Units' : str(data.variables[str(x)].units), 
                                        'scale_factor' : check_scale(x)},  ignore_index = True) 

#Checking data types - to be used for schema later

""" print(type(longitude_data))
print(type(latitude_data))
print(type(u10_data))
print(type(v10_data))  """

# Zipping latitudes and longitudes

#u10_data =  zip(time_data,data.variables['u10'][:])
#v10_data =  zip(time_data,v10_data)

#Creating data frames

df_time = pd.DataFrame(time_data, columns = ['Time'])
df_lat = pd.DataFrame(latitude_data, columns = ['lat'] )
df_lon = pd.DataFrame(longitude_data, columns = ['lon'] )

#df_u = pd.DataFrame(data.variables['u10'][:], columns = ['Time','u10_data'])
#df_v = pd.DataFrame(v10_data, columns = ['Time','v10_data'])

#df_u['u10_data'] = df_u['u10_data'].apply(lambda x: x.tolist())

#df.index.name = 'Time'

#print(len(df_u['u10_data'][0][0]))

#Postgres connection

dbschema = 'buffer'
#engine = create_engine('postgresql+psycopg2://sputnik:starman@127.0.0.1:5432/starlink')
engine = create_engine('postgresql+psycopg2://postgres:miarujahhahh@40.89.177.14:5432/aiu_db',connect_args={'options': '-csearch_path={}'.format(dbschema)})

def write_data(name,df):
    df.to_sql(f"netcdf_{name}", engine, if_exists='replace',index=False)

write_data('time',df_time)
write_data('lon',df_lon)
write_data('lat',df_lat)
write_data('metadata', df_meta_data)

#write_data('u',df_u)
#write_data('v',df_v)

end_time = time.time()
print("Execution time: ", end_time - start_time)
import os, psutil; print(psutil.Process(os.getpid()).memory_info().rss/1024**2)
""" 
print(data.variables['time'])
print(data.variables['longitude'])
print(data.variables['latitude'])
print(data.variables['u10'])
print(data.variables['v10'])
 """
