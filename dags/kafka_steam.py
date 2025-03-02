from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import requests
from kafka import KafkaProducer
import time
API_key = 'Your_API_KEY' # your api key
# lat = 13.75 # latitude # Bangkok
# lon = 100.5167 # longitude # Bangkok
bangkok_districts = {
    "Phra Nakhon": {"latitude": 13.7563, "longitude": 100.5018},
    "Dusit": {"latitude": 13.7842, "longitude": 100.5117},
    "Nong Chok": {"latitude": 13.8583, "longitude": 100.8659},
    "Bang Rak": {"latitude": 13.7259, "longitude": 100.5283},
    "Bang Khen": {"latitude": 13.8644, "longitude": 100.5843},
    "Bang Kapi": {"latitude": 13.7554, "longitude": 100.6309},
    "Pathum Wan": {"latitude": 13.7445, "longitude": 100.5342},
    "Pom Prap Sattru Phai": {"latitude": 13.7504, "longitude": 100.5097},
    "Phra Khanong": {"latitude": 13.7088, "longitude": 100.6022},
    "Min Buri": {"latitude": 13.8053, "longitude": 100.7550},
    "Lat Krabang": {"latitude": 13.7262, "longitude": 100.7562},
    "Yannawa": {"latitude": 13.6896, "longitude": 100.5369},
    "Samphanthawong": {"latitude": 13.7408, "longitude": 100.5137},
    "Phaya Thai": {"latitude": 13.7719, "longitude": 100.5385},
    "Thon Buri": {"latitude": 13.7229, "longitude": 100.4907},
    "Bangkok Yai": {"latitude": 13.7314, "longitude": 100.4806},
    "Huai Khwang": {"latitude": 13.7788, "longitude": 100.5759},
    "Khlong San": {"latitude": 13.7348, "longitude": 100.5042},
    "Taling Chan": {"latitude": 13.7702, "longitude": 100.4429},
    "Bangkok Noi": {"latitude": 13.7634, "longitude": 100.4735},
    "Bang Khun Thian": {"latitude": 13.6703, "longitude": 100.4578},
    "Phasi Charoen": {"latitude": 13.7214, "longitude": 100.4357},
    "Nong Khaem": {"latitude": 13.7153, "longitude": 100.3792},
    "Rat Burana": {"latitude": 13.6865, "longitude": 100.4925},
    "Bang Phlat": {"latitude": 13.7951, "longitude": 100.5015},
    "Din Daeng": {"latitude": 13.7663, "longitude": 100.5562},
    "Bueng Kum": {"latitude": 13.7895, "longitude": 100.6558},
    "Sathon": {"latitude": 13.7211, "longitude": 100.5289},
    "Bang Sue": {"latitude": 13.8009, "longitude": 100.5297},
    "Chatuchak": {"latitude": 13.8166, "longitude": 100.5557},
    "Prawet": {"latitude": 13.7196, "longitude": 100.6698},
    "Khlong Toei": {"latitude": 13.7126, "longitude": 100.5582},
    "Suan Luang": {"latitude": 13.7212, "longitude": 100.6406},
    "Chom Thong": {"latitude": 13.6853, "longitude": 100.4674},
    "Don Mueang": {"latitude": 13.9125, "longitude": 100.5989},
    "Ratchathewi": {"latitude": 13.7547, "longitude": 100.5342},
    "Lat Phrao": {"latitude": 13.8078, "longitude": 100.5701},
    "Watthana": {"latitude": 13.7369, "longitude": 100.5847},
    "Sai Mai": {"latitude": 13.9248, "longitude": 100.6385},
    "Khan Na Yao": {"latitude": 13.8159, "longitude": 100.6669},
    "Saphan Sung": {"latitude": 13.7610, "longitude": 100.6726},
    "Wang Thonglang": {"latitude": 13.7889, "longitude": 100.6120},
    "Khlong Sam Wa": {"latitude": 13.8647, "longitude": 100.7351},
}
default_arg = {
    'owner' : 'copter',
    'start_date': datetime(2025,2,19, 12, 00)
}
def get_data(lat, lon):
    url = f'http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={API_key}'
    res = requests.get(url)
    res = res.json()
    return res

def format_data(res,name): # format the data to be sent to kafka
    data = {}
    data['district'] = name
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
        for index, row in bangkok_districts.items():
            name = index
            lat = row['latitude']
            lon = row['longitude']
            if time.time() > curr_time + 100: # 3 minutes
                break
            try:
                res = get_data(lat, lon)
                data = format_data(res, name)
                producer.send('airpollution', json.dumps(data).encode('utf-8')) # utf-8 means 8-bit Unicode Transformation Format
                logging.info('Data sent to kafka')
                # time.sleep(10)  # Sleep for 10 seconds
            except Exception as e:
                logging.error(f"Error in sending data to kafka due to {e}")
                continue
        time.sleep(30)  # Sleep for 30 seconds
            
        # if time.time() > curr_time + 100:  # 3 minutes
        #     break
        # try:
        #     res = get_data()
        #     data = format_data(res)
        #     producer.send('airpollution', json.dumps(data).encode('utf-8')) # utf-8 means 8-bit Unicode Transformation Format
        #     logging.info('Data sent to kafka')
        #     time.sleep(10)  # Sleep for 10 seconds
        # except Exception as e:
        #     logging.error(e)
        #     continue
            
    
            
    
with DAG('airpollution',
         default_args=default_arg,
         schedule_interval='@daily', # run the DAG daily
         catchup=False) as dag:
    
    steaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=steam_data
    )