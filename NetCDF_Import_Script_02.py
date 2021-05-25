import os, os.path, itertools, datetime, json, tempfile, gc

import numpy as np
import xarray as xr
import pandas as pd
#import rasterio as rio
import rioxarray as riox
import subprocess
import psycopg2
#os.environ["GDAL_DATA"] = os.path.join(os.environ["CONDA_PREFIX"], r"Library/share/gdal")


class netcdf:
    
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
    
    
    def rasterize(self, time="time", latitude="latitude", longitude="longitude", drop_coords=None, num_rasters=1, raster_type="GeoTIFF"):
        """
        Generator that yields rasters
        gis_coords = names of coordinates that contain timestamp/datetime, latitude and longitude
        drop_coords = redundant or uniquely valued coordinates to drop
        """
        
        #Database connection

        db_name = 'aiu_db'
        db_host = '40.89.177.14'
        db_user = 'postgres'
        db_password = 'miarujahhahh'
        db_tablename = 'netcdf_rasters'
        os.environ['PGPASSWORD'] = db_password
        
        con = psycopg2.connect(database=db_name, user=db_user, password=db_password, host=db_host, port="5432")
        cur = con.cursor()


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
                        check_table=cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE  table_schema = 'public' AND table_name= str({}));".format(db_tablename)) 
                        con.commit()
                        if not check_table:

                            cmd = 'raster2pgsql -s 4326 -C -F -t auto {}* {} | psql -U {} -d {} -h {} -p 5432'.format(path,db_tablename,db_user,db_name,db_host)
                            yield rasters
                        #if not check_table(db_tablename):
                        #try:
                            subprocess.call(cmd, shell=True)
                       # except subprocess.CalledProcessError:
                       #     return('Table exists')

                        else:
                            print('table exists')
                        rasters.clear()
            
        if len(rasters)>0:
#            cmd = 'raster2pgsql -s 4326 -C -F -t path*.tif netcdf_raster | psql -U {} -d {} -h {} -p 5432'.format(db_user,db_name,db_host)
            yield rasters
#            subprocess.call(cmd, shell=True)
            rasters.clear()

        
data = netcdf('/mnt/data/rasters/WindData2020_03_13_to_26.nc')

#for x in data.rasterize():
#    print(x)
 
for x in data.rasterize():
    print(x)
