import os
import requests
import datetime
import time
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)
API_KEY = os.getenv('key')

cities = ['Moscow', 'Saint Petersburg', 'Belgorod']

BASE_URL = 'http://api.weatherapi.com/v1/history.json'

end_date = datetime.datetime.now()
start_date = end_date - datetime.timedelta(days=30)

def get_historical_weather(city, date):
    params = {
        'key': API_KEY,
        'q': city,
        'dt': date.strftime('%Y-%m-%d')
    }
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Ошибка при получении данных для {city} на дату {date.strftime('%Y-%m-%d')}: {response.status_code}")
        return None

weather_data_list = []

for city in cities:
    print(f"Сбор данных для города: {city}")
    current_date = start_date
    while current_date <= end_date:
        data = get_historical_weather(city, current_date)
        if data:
            date = data['forecast']['forecastday'][0]['date']
            avg_temp = data['forecast']['forecastday'][0]['day']['avgtemp_c']
            weather_data_list.append({
                'city': city,
                'date': date,
                'avg_temp': avg_temp
            })
        current_date += datetime.timedelta(days=1)
        time.sleep(1)


weather_data = pd.DataFrame(weather_data_list)

weather_data['date'] = pd.to_datetime(weather_data['date'])

# Построение графика изменения температуры
plt.figure(figsize=(14, 7))
for city in cities:
    city_data = weather_data[weather_data['city'] == city]
    plt.plot(city_data['date'], city_data['avg_temp'], label=city)
plt.xlabel('Дата')
plt.ylabel('Средняя температура (°C)')
plt.title('Изменение средней температуры за последние 30 дней')
plt.legend()
plt.grid(True)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Построение гистограммы распределения температуры
plt.figure(figsize=(14, 7))
for city in cities:
    city_data = weather_data[weather_data['city'] == city]
    plt.hist(city_data['avg_temp'], bins=15, alpha=0.5, label=city)
plt.xlabel('Средняя температура (°C)')
plt.ylabel('Частота')
plt.title('Распределение средней температуры за последние 30 дней')
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()

def save_to_hdfs(df, hdfs_path):
    hdfs = fs.HadoopFileSystem('localhost', port=8020, user='inna')
    table = pa.Table.from_pandas(df)
    with hdfs.open_output_stream(hdfs_path) as out_stream:
        pq.write_table(table, out_stream)
    print(f"Данные успешно сохранены в HDFS по пути: {hdfs_path}")
    return hdfs

hdfs_path = '/user/inna/weather_data.parquet'
local_file_path_csv = "weather_data.csv"
local_file_path_parquet = "weather_data.parquet"
hdfs = save_to_hdfs(weather_data, hdfs_path)

def download_from_hdfs(hdfs_path, local_csv_path, local_parquet_path):
    try:
        with hdfs.open_input_stream(hdfs_path) as in_stream:
            table = pq.read_table(in_stream)

        df = table.to_pandas()

        df.to_csv(local_csv_path, index=False, encoding='utf-8')
        print(f"Файл сохранен локально: {local_csv_path}")

        df.to_parquet(local_parquet_path, index=False)
        print(f"Файл сохранен локально: {local_parquet_path}")

    except Exception as e:
        print(f"Ошибка при загрузке из HDFS: {e}")

download_from_hdfs(hdfs_path, local_file_path_csv, local_file_path_parquet)