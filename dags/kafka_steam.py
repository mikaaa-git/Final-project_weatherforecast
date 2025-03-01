from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import requests
API_key = '###########################' # your api key
lat = 13.75 # latitude
lon = 100.5167 # longitude
url = f'http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={API_key}'
default_arg = {
    'owner' : 'copter',
    'start_date': datetime(2025,2,19, 12, 00)
}
def get_data():
    res = requests.get(url)
    res = res.json()
    return res

def format_data(res):
    data = {}
    data['timestamp'] = res['list'][0]['dt']
    data['lon'] = res['coord']['lon']
    data['lat'] = res['coord']['lat']
    data['AQI'] = res['list'][0]['main']['aqi']
    data['CO'] = res['list'][0]['components']['co']
    data['NO'] = res['list'][0]['components']['no']
    data['NO2'] = res['list'][0]['components']['no2']
    data['O3'] = res['list'][0]['components']['o3']
    data['SO2'] = res['list'][0]['components']['so2']
    data['PM2_5'] = res['list'][0]['components']['pm2_5']
    data['PM10'] = res['list'][0]['components']['pm10']
    data['NH3'] = res['list'][0]['components']['nh3']
    return data
def steam_data():
    res = get_data()
    data = format_data(res)
    print(json.dumps(data, indent=4))
    

def format_data(res):
    data = {}
    data['timestamp'] = res['list'][0]['dt']
    data['lon'] = res['coord']['lon']
    data['lat'] = res['coord']['lat']
    data['AQI'] = res['list'][0]['main']['aqi']
    data['CO'] = res['list'][0]['components']['co']
    data['NO'] = res['list'][0]['components']['no']
    data['NO2'] = res['list'][0]['components']['no2']
    data['O3'] = res['list'][0]['components']['o3']
    data['SO2'] = res['list'][0]['components']['so2']
    data['PM2_5'] = res['list'][0]['components']['pm2_5']
    data['PM10'] = res['list'][0]['components']['pm10']
    data['NH3'] = res['list'][0]['components']['nh3']
    return data
def steam_data():
    import logging
    # print(json.dumps(data, indent=4))
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000) # connect to kafka
    #send data to kafka every 3mins
    curr_time = time.time()
    while True:
        if time.time() > curr_time + 100:
            break
        try:
            res = get_data()
            data = format_data(res)
            producer.send('airpollution', json.dumps(data).encode('utf-8'))
            logging.info('Data sent to kafka')
            time.sleep(100) # sleep for 100s
        except Exception as e:
            logging.error(e)
            continue