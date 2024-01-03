import glob
import os
import pandas as pd
import json
from modules.ga_hits_to_sql import pipeline_ga_hits, pipeline_ga_hits_new

# Укажем путь к файлам проекта:
# -> $PROJECT_PATH при запуске в Airflow
# -> иначе - текущая директория при локальном запуске
path = os.environ.get('PROJECT_PATH', '..')

def pipeline():
    for test_model in glob.glob(f'{path}/data/json_files/ga_hits_new/*.json'): #  Проходим циклом по директории
        try:
            with open(test_model) as file:
                data = json.load(file)
                for k, v in data.items():
                    di = dict(enumerate(v)) # получаем одновременно и индекс элемента и его значение
                    df = pd.DataFrame.from_dict(di, orient='index') # Преобразовываем словарь в Датафрейм
                    data = pipeline_ga_hits(df)
                    pipeline_ga_hits_new(data)
        except Exception as ex:
            print('The file has not been added to the table')
            continue

#pipeline()
