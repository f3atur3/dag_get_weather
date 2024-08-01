#############################################################
# Команды git для управления репозиторием:
#   инициализация:
# git init
# 
#   создание ветки develop
# git checkout -b develop
# 
#   создание ветки feature/add-dag от ветки develop
# git checkout -b feature/add-dag develop
# 
#   добавление файлов в индекс и коммит
# git add .
# git commit -m "Add initial DAG for weather data pipeline"
# 
#############################################################


import json
import os
from datetime import datetime

import pandas as pd
import requests
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

PATH_JSON_FILE = 'tmp/weather_data.json'
PATH_CSV_FILE = 'processed_weather_data.csv'
PATH_PARQUET_FILE = 'weather.parquet'

API_KEY = '796bf5e7e686bc20acc7a63415b09e48'

def download_data():
    global API_KEY, PATH_JSON_FILE
    
    # Указываем url и необхожимые параметры запроса
    url = 'https://api.openweathermap.org/data/2.5/weather'
    params = {
        'q': 'London',
        # 'units': 'metric',
        'appid': API_KEY
    }
    cur_date = str(datetime.datetime.now())
    
    # Делаем GET запрос к API (время от времени API долго отвечает,
    # из-за чего возникает ошибка TimeoutError)
    resp = requests.get(url, params=params, timeout=(60, 60))
    
    # Берем из ответа блок main, который содержит в себе информацию
    # о текущей, воспринимаемой человеком, минимальной и максимальной
    # температурах, атмосферном давлении на уровне моря и на уровене
    # земли и вланжности. Из него извлекаем только температуру и 
    # добавляем к нему информаци о текущей дате
    main_data: dict = resp.json().get('main', {})
    fields = ['temp', 'feels_like', 'temp_min', 'temp_max']
    data_to_json = {field: [main_data.get(field, None)] for field in fields}
    data_to_json.update({'date': [cur_date]})
    
    # Если папка tmp не существует, то она будет создана
    os.makedirs(os.path.dirname(PATH_JSON_FILE), exist_ok=True)
    
    # Загружаем данные в json файл
    with open(PATH_JSON_FILE, 'w') as json_file:
        json.dump(data_to_json, json_file)

def process_data():
    global PATH_JSON_FILE, PATH_CSV_FILE
    
    # Считываем данные из json в pandas.DataFrame
    df = pd.read_json(PATH_JSON_FILE)
    
    # Преобразуем все поля с температурой из Кельвинов в Цельсий
    temp_fileds = ['temp', 'feels_like', 'temp_min', 'temp_max']
    for field in temp_fileds:
        df[field] -= 273.15
    
    # Сохраняем обработанные данные в csv файл без индексов DataFrame
    df.to_csv(PATH_CSV_FILE, index=False)

def save_data():
    global PATH_CSV_FILE, PATH_PARQUET_FILE
    
    # Считываем обработанные данные из csv файла
    df = pd.read_csv(PATH_CSV_FILE)
    
    # Сохраняем данные в формате parquet без индексов DataFrame
    df.to_parquet(PATH_PARQUET_FILE, index=False)

# Создаем  DAG
dag = DAG("get_weather_dag", start_date=datetime(2024, 8, 11))

# Обявляем задачи
task1 = PythonOperator(task_id="task1", python_callable=download_data, dag=dag)
task2 = PythonOperator(task_id="task2", python_callable=process_data, dag=dag)
task3 = PythonOperator(task_id="task3", python_callable=save_data, dag=dag)

# Определяем порядок выполнения
task1 >> task2
task2 >> task3