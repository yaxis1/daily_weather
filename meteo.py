import json
import requests
import psycopg2
import pandas as pd
from psycopg2 import connect
from sqlalchemy import create_engine
import io

lat = 48.86
lon = 2.34
part = "current,minutely,hourly,alerts"
API_key = ''


url = f"https://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lon}&exclude={part}&appid={API_key}&units=metric"

payload={}
headers = {}    
response = requests.request("GET", url, headers=headers, data=payload)
daily_weather = response.json()["daily"]

#Save response to csv 
with open('daily_weather.json', 'w') as outfile:
    json.dump(daily_weather, outfile) 

df = pd.read_json(r'daily_weather.json') #Data frame

engine = create_engine('postgresql+psycopg2://sputnik:starman@127.0.0.1:5432/starlink')
 
def write_data(df):
    df.head(0).to_sql(f"meteo", engine, if_exists='append',index=False) #appends daily weather data
    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    contents = output.getvalue()
    cur.copy_from(output, f'meteo', null="") # null values become ''
    conn.commit()
    
write_data(df)

