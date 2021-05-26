import os,io, os.path, itertools, datetime, json, tempfile, gc, psutil
from netCDF4 import Dataset
import numpy as np
import xarray as xr
import pandas as pd
#import rasterio as rio
#import rioxarray as riox
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
        self.df_time = pd.DataFrame(self.data.variables['time'][:], columns = ['Time'])
        self.df_lat = pd.DataFrame(self.data.variables['latitude'][:], columns = ['lat'] )
        self.df_lon = pd.DataFrame(self.data.variables['longitude'][:], columns = ['lon'] )
        self.engine = create_engine('postgresql+psycopg2://postgres:miarujahhahh@40.89.177.14:5432/aiu_db',connect_args={'options': '-csearch_path={}'.format(db_schema)})

                

    def check_scale(self,x):
        if hasattr(self.data.variables[str(x)],'scale_factor'):
            return (self.data.variables[str(x)].scale_factor)
        else: 
            return ('none')

    def df_meta_data(self):
        df_meta_data = pd.DataFrame(columns=['Long Name','Units'])

        for x in self.data.variables.keys() :
            df_meta_data = df_meta_data.append({'Long Name' : self.data.variables[str(x)].long_name, 
                                                'Units' : str(self.data.variables[str(x)].units), 
                                                'scale_factor' : self.check_scale(x)},  ignore_index = True)
            return df_meta_data 

    
    def write_data(self,name,df):
        df.to_sql(f"netcdf_{name}", self.engine, if_exists='replace',index=False)




class netcdf:

 #Database connection
    def __init__(self, *args, **kwargs):
        """
        Open file
        """
        self.dataset = xr.open_dataset(*args, **kwargs)
        self.is_open = True        
        
    
    def close(self):
        self.dataset.close()
        self.is_closed = False
        
    
    @staticmethod
    def normalize(signal, intr_coords, intr_values):
        """
        Combine the signal's name with the values of the intrinsic coordinates for the new, complete signal name
        Inserts the new signal name (json string) in the table self.signals, and return the corresponding signal_id value
        """
        if intr_coords==None:
            intr_coords = set()
        if intr_values==None:
            intr_values = set()
        
        if len(intr_coords)!=len(intr_values):
            raise ValueError()
        
        if len(intr_coords)==0:
            signal_name = json.dumps([signal])
        else:
            signal_name = json.dumps([signal, {c:v.item() for c,v in zip(intr_coords, intr_values)}])
            
        return signal_name
    
    
    def rasterize(self, db_raster, time="time", latitude="latitude", longitude="longitude", drop_coords=None, num_rasters=1, raster_type="GeoTIFF"):
        """
        Generator that yields rasters
        gis_coords = names of coordinates that contain timestamp/datetime, latitude and longitude
        drop_coords = redundant or uniquely valued coordinates to drop
        """
        
        if num_rasters!=None and num_rasters<=0:
            raise ValueError()
        
        gis_coords = {time, latitude, longitude} & set(self.dataset.coords)
        
        if drop_coords==None:
            drop_coords = set()
        else:
            drop_coords = set(drop_coords)
            
        if not drop_coords<=set(self.dataset.coords):
            raise KeyError()
        
        if gis_coords&drop_coords!=set():
            raise KeyError()
            
        intr_coords = set(self.dataset.coords) - (gis_coords|drop_coords)
        
        temp_dir = tempfile.TemporaryDirectory()
        
        rasters = []

        #Checking relation
        def check_rasters(db_raster):
            con = psycopg2.connect(database=db_name, user=db_user, password=db_password, host=db_host, port="5432")
            cur = con.cursor()
            cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE  table_schema = '{}' AND table_name= '{}' );".format(db_schema,db_raster) )
            for x in cur.fetchone():
                return (x)
        
        for t in self.dataset[time].values:
            for dat in self.dataset.data_vars:
                for intr_vals in itertools.product(*[self.dataset[intr].values for intr in intr_coords]):
                    
                    signal = self.normalize(dat, intr_coords, intr_vals)

                    da = self.dataset[dat].sel(time=t, **dict(zip(intr_coords, intr_vals)))

                    for ic in intr_coords|{time}:
                        da = da.drop(ic)

                    for dc in drop_coords&set(da.coords):
                        da = da.reduce(np.nanmin, dim=dc)
                        
                    da = da.assign_coords(**{longitude: (da[longitude]+180)%360-180}).sortby(longitude)
                    
                    
                    if raster_type.lower()=="geotiff":
                        da.rio.set_spatial_dims(longitude, latitude)
                        da.rio.set_crs("EPSG:4326")
                        path = os.path.join(temp_dir.name, f"raster_file_{len(rasters)+1}.tif")
                        da.rio.to_raster(path)
                        
                    elif raster_type.lower()=="netcdf":
                        path = os.path.join(temp_dir.name, f"raster_file_{len(rasters)+1}.nc")
                        da.to_netcdf(path)
                        
                    rasters.append( (t, signal, path) )
                    
                    if num_rasters!=None and len(rasters)>=num_rasters:
                        if not check_rasters():
                            cmd = 'raster2pgsql -s 4326 -C -F -t auto {}* {}.{} | psql -U {} -d {} -h {} -p 5432'.format(path,db_schema,db_raster,db_user,db_name,db_host)
                            yield rasters
                            subprocess.call(cmd, shell=True)
                        else:
                            return('table exists')
                        rasters.clear()
            
        if len(rasters)>0:
            yield rasters
            rasters.clear()

        
data1 = netcdf_cols('WindData2020_03_13_to_26.nc')

#data = netcdf('/mnt/data/rasters/WindData2020_03_13_to_26.nc')

#Creating columns
data1.write_data('time',data1.df_time)
data1.write_data('lon',data1.df_lon)
data1.write_data('lat',data1.df_lat)
data1.write_data('metadata', data1.df_meta_data())

#Creating rasters
#for x in data.rasterize('netcdf_rasters'):
#    print(x)

end_time = time.time()
print("Execution time: ", end_time - start_time)
print(psutil.Process(os.getpid()).memory_info().rss/1024**2)



