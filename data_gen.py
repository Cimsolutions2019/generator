import math
import random
import time
import petl as etl
import psycopg2
import pandas as pd
from sqlalchemy  import create_engine
from datetime import datetime
from datetime import timedelta
import numpy as np

connection = psycopg2.connect(dbname='voyager', user='voy', password='voyager', host='172.16.0.45')
engine = create_engine('postgresql://voy:voyager@172.16.0.45:5432/voyager')

res = "select id, name from research "
res_table = etl.fromdb(connection, res)
for id,name in res_table:
    print(id, name)

print('Kies een research getal')
research = input(' ')

voyager_sql = "select id, name from research_voyager r INNER JOIN voyagers v ON r.voyager_id = v.id WHERE research_id = " + research
voy_table = etl.fromdb(connection, voyager_sql)

for id,name in voy_table:
    print(id, name)

print('Selecteer voyager')
voyager = input()

print('Sensoren aan het ophalen van: ', research)
selected_research = res = "select * from research where id="+research
research_table = etl.fromdb(connection, selected_research)



select_voyagers = "select sensors from voyager_sensors vs \
inner join research_voyager rv on vs.voyager_id = rv.voyager_id \
inner join research r on rv.research_id = r.id \
where r.id =  " + research + " and vs.voyager_id = " + voyager

sensor_table = etl.fromdb(connection, select_voyagers)
print(sensor_table)
sensors = pd.DataFrame(sensor_table)
sensors.columns = sensors.iloc[0]
sensors = sensors.drop(0)
sensors = sensors.set_index('sensors')

fmt = '%Y-%m-%d %H:%M:%S'
start_date = datetime.strptime(research_table['start_date'][0].strftime('%Y-%m-%d %H:%M:%S'), fmt)
end_date = datetime.strptime(research_table['end_date'][0].strftime('%Y-%m-%d %H:%M:%S'), fmt)
start_temp = 15
start_hum = 48
lat = 52.466005
long = 4.932636
print('start date: ', start_date)
print('end date: ', end_date)

# Convert to Unix timestamp
start_ts = time.mktime(start_date.timetuple())
end_ts = time.mktime(end_date.timetuple())

iterations = math.floor((end_ts-start_ts)/60)+1
# They are now in seconds, subtract and then divide by 60 to get minutes.
print("Minutes to be generated: ",  iterations)

# print(sensors['sensors'])

def temp_humid_gen():
    df = pd.DataFrame()
    past_temp = start_temp
    past_hum = start_hum
    for i in range(0, iterations, 1):
        time = start_date + timedelta(seconds=60*i)
        if random.randint(0,1) > 0:
            temperature = past_temp + 1
            humidity = past_hum + 0.1
        else :
            temperature = past_temp - 1
            humidity = past_hum - 0.1
        row = pd.DataFrame([[time, temperature, humidity, lat, long]], columns=['time', 'temperature', 'humidity', 'latitude', 'longitude'])
        df = df.append(row)
        past_temp = temperature
        past_hum = humidity
    df = df.reset_index()
    df = df.drop('index', axis=1)
    df = df.reset_index()
    insert_data(df, 1)


def temp_gen():
    df = pd.DataFrame()
    past_temp = start_temp
    for i in range(0, iterations, 1):
        time = start_date + timedelta(seconds=60 * i)
        if random.randint(0, 1) > 0:
            temperature = past_temp + 1
        else:
            temperature = past_temp - 1
        row = pd.DataFrame([[time, temperature,  lat, long]],
                           columns=['time', 'temperature',  'latitude', 'longitude']).reset_index()
        df = df.append(row)
        past_temp = temperature
    df = df.reset_index()
    df = df.drop('index', axis=1)
    df = df.reset_index()
    insert_data(df, 2)


def humid_gen():
    df = pd.DataFrame()
    past_hum = start_hum
    for i in range(0, iterations, 1):
        time = start_date + timedelta(seconds=60 * i)
        if random.randint(0, 1) > 0:
            humidity = past_hum + 0.1
        else:
            humidity = past_hum - 0.1
        row = pd.DataFrame([[time,  humidity, lat, long]],
                           columns=['time', 'temperature', 'humidity', 'latitude', 'longitude'])
        df = df.append(row)
        past_hum = humidity
    df = df.reset_index()
    df = df.drop('index', axis=1)
    df = df.reset_index()
    insert_data(df,3)

def insert_data(df, int):

    connection = psycopg2.connect(dbname='voyager', user='voy', password='voyager', host='172.16.0.45')
    engine = create_engine('postgresql://voy:voyager@172.16.0.45:5432/voyager')

    latest_temp = 'SELECT id FROM temperature ORDER BY id DESC LIMIT 1'
    latest_hum = 'SELECT id FROM humidity ORDER BY id DESC LIMIT 1'
    temp = etl.fromdb(connection, latest_temp)
    hum = etl.fromdb(connection, latest_hum)
    temp = temp['id'][0] + 1
    hum = hum['id'][0] + 1


    df_loc = df[['time', 'latitude', 'longitude']]

    if int == 1:
        df_temp = df[['temperature']]
        df_temp.index = np.arange(temp, len(df_temp) + temp)
        df_temp['id'] = df_temp.index
        df_temp.rename(columns={'temperature': 'value'}, inplace=True)
        df_loc['temperature_id'] = df_temp.index
        df_temp.to_sql('temperature', engine, if_exists='append', index=False, method='multi')

        print(df_temp)
        df_hum = df[['humidity']]
        df_hum.index = np.arange(hum, len(df_hum) + hum)
        df_hum['id'] = df_hum.index
        df_hum.rename(columns={'humidity': 'value'}, inplace=True)
        df_loc['humidity_id'] = df_hum.index
        print(df_hum)
        df_hum.to_sql('humidity', engine, if_exists='append', index=False, method='multi')

    if int == 2:
        df_temp = df[['temperature']]
        df_temp.index = np.arange(temp, len(df_temp) + temp)
        df_temp['id'] = df_temp.index
        df_temp.rename(columns={'temperature': 'value'}, inplace=True)
        df_loc['temperature_id'] = df_temp.index
        print(df_temp)

        df_temp.to_sql('temperature', engine, if_exists='append', index=False, method='multi')

    if int == 3:
        df_hum = df[['humidity']]
        df_hum.index = np.arange(hum, len(df_hum) + hum)
        df_hum['id'] = df_hum.index
        df_hum.rename(columns={'humidity': 'value'}, inplace=True)
        df_loc['humidity_id'] = df_hum.index
        print(df_hum)
        df_hum.to_sql('humidity', engine, if_exists='append', index=False, method='multi')


    df_loc['research_id'] = research
    df_loc['voyager_id'] = voyager
    print(df_loc)
    df_loc.to_sql('location', engine, if_exists='append', index=False, method='multi')














if 'TEMPERATURE' in sensors.index and 'HUMIDITY' in sensors.index:
    print('Found temperature and humidity')
    temp_humid_gen()
else :
    if 'TEMPERATURE' in sensors.index:
        print('Found temperature')
        temp_gen()
    if 'HUMIDITY' in sensors.index:
        print('Found humidity')
        humid_gen()