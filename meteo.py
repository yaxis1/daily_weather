#api = "228d035930bf3aa35173b0fb636da8ba"

#open_weather = f'http://api.openweathermap.org/data/2.5/forecast?id=524901&appid={api}'

import json
import csv
import requests
import pandas as pd

lat = 48.86
lon = 2.34
part = "current,minutely,hourly,alerts"
API_key = ""


url = f"https://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lon}&exclude={part}&appid={API_key}&units=metric"

payload={}
headers = {}    

response = requests.request("GET", url, headers=headers, data=payload)
daily_weather = response.json()["daily"]

#Save response to csv 
with open('daily_weather.json', 'w') as outfile:
    json.dump(daily_weather, outfile) 

df = pd.read_json (r'daily_weather.json')
df.to_csv (r'weather.csv', mode = 'a', index = None)
 
