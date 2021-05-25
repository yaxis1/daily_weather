import os,io, os.path, itertools, datetime, json, tempfile, gc, psutil
from netCDF4 import Dataset
import numpy as np
import xarray as xr
import pandas as pd
#import rasterio as rio
import subprocess

from sqlalchemy import create_engine
import psycopg2 
import time

#os.environ["GDAL_DATA"] = os.path.join(os.environ["CONDA_PREFIX"], r"Library/share/gdal")
start_time = time.time()

db_name = 'aiu_db'
db_host = '40.89.177.14'
db_user = 'postgres'
db_password = 'miarujahhahh'
db_schema = 'buffer'
os.environ['PGPASSWORD'] = db_password

class netcdf_cols:

    def __init__(self,path):
        self.data = Dataset(path)
        self.time_data = self.data.variables['time'][:]
        self.longitude_data = self.data.variables['longitude'][:]
        self.latitude_data = self.data.variables['latitude'][:]
                
        self.df_time = pd.DataFrame(self.time_data, columns = ['Time'])
        self.df_lat = pd.DataFrame(self.latitude_data, columns = ['lat'] )
        self.df_lon = pd.DataFrame(self.longitude_data, columns = ['lon'] )
    
    def check_scale(self,x):
        if hasattr(data.variables[str(x)],'scale_factor'):
            return (data.variables[str(x)].scale_factor)
        else:
            return ('None')


    df_meta_data = pd.DataFrame(columns=['Long Name','Units'])

    for x in data.variables.keys(self):
        df_meta_data = df_meta_data.append({'Long Name' : data.variables[str(x)].long_name, 
                                            'Units' : str(data.variables[str(x)].units), 
                                            'scale_factor' : check_scale(x)},  ignore_index = True) 

    engine = create_engine('postgresql+psycopg2://postgres:miarujahhahh@40.89.177.14:5432/aiu_db',connect_args={'options': '-csearch_path={}'.format(dbschema)})
    
    def write_data(self,name,df):
        df.to_sql(f"netcdf_{name}", engine, if_exists='replace',index=False)
 


dataset = netcdf_cols('WindData2020_03_13_to_26.nc')

print(dataset.data)