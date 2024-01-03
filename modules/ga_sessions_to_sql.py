import os
import pandas as pd
import pymysql
from config import host,user,password,db_name

# Укажем путь к файлам проекта:
# -> $PROJECT_PATH при запуске в Airflow
# -> иначе - текущая директория при локальном запуске
path = os.environ.get('PROJECT_PATH', '..')
#df = pd.read_csv(f'{path}/data/csv_files/ga_sessions_new.csv',dtype=str)


def filter_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df[['session_id', 'client_id', 'visit_date', 'visit_time', # Выбор значимых атрибутов для анализа данных
                'utm_source', 'utm_medium', 'utm_campaign',
                'utm_adcontent', 'device_category', 'device_brand',
                'device_browser', 'geo_country', 'geo_city'
    ]]

    return df


def drop_dublicates(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates()   # Удаляем дубликаты в таблице

    return df

def drop_null(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df.utm_source.notna()]  # принимаем решение,удалив записи,в которых канал привлечения не заполнен

    return df

def refactor(df: pd.DataFrame) -> pd.DataFrame:
    # Обрабатываем пропуски, заполнив незаполненные значения марки устройства значением 'none' , и сохраняем изменения
    df['device_brand'] = df['device_brand'].fillna('(not set)')
    df['device_brand'] = df['device_brand'].replace('', '(not set)')

    return df

def refactor_null(df: pd.DataFrame) -> pd.DataFrame:
    # Обрабатываем пропуски, заполнив незаполненные значения марки устройства значением 'none' , и сохраняем изменения
    df['utm_campaign'] = df['utm_campaign'].fillna('(none)')

    return df

def refactor_null2(df: pd.DataFrame) -> pd.DataFrame:
    # Обрабатываем пропуски, заполнив незаполненные значения марки устройства значением 'none' , и сохраняем изменения
    df['utm_adcontent'] = df['utm_adcontent'].fillna('(none)')

    return df

def replace(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df['client_id'] = df.client_id.apply(lambda x: x.replace('.', '')) # Убираем точки в записях колонок
    df['session_id'] = df.session_id.apply(lambda x: x.replace('.', ''))

    return df

def date_join_time(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # объединяем две колонки,чтобы получить одну запись даты и времени для MySQL
    df['Date_time'] = df.apply(lambda x: ' '.join([x.visit_date, x.visit_time]), axis=1)

    return df


def type_datetime(df: pd.DataFrame) -> pd.DataFrame:
    df['Date_time'] = pd.to_datetime(df['Date_time']) # меняем тип данных в признаке дата и время

    return df


def trafic_result(df: pd.DataFrame) -> pd.DataFrame:
    traffic_result = ['organic','referral', '(none)']
    # Новый признак из двух значений: платный трафик визита или нет
    df['utm_medium_new'] = df.apply(lambda x: 'organic traffic' if x.utm_medium in traffic_result else 'paid traffic',
                                    axis=1)
    return df

def social_network(df: pd.DataFrame) -> pd.DataFrame:
    social = ['QxAxdyPLuQMEcrdZWdWb', 'MvfHsxITijuriZxsqZqt', 'ISrKoXQCxqqYvAZICvjs',
              'IZEXUFLARCUMynmHNBGo', 'PlbkrSYoHuZBWfYjYnfw',
              'gVRrcxiDQubJiljoTbGm']
    # новый признак из двух значений: реклама в соцсетях или нет
    df['utm_source_new'] = df.apply(lambda x: 'social_network' if x.utm_source in social else 'advertisement',axis=1)

    return df

def mobile_or_desktop(df: pd.DataFrame) -> pd.DataFrame:
    mobile = ['mobile', 'tablet']
    # новый признак из двух значений: мобильное смарт устройство или нет
    df['device_category_new'] = df.apply(lambda x: 'mobile' if x.device_category in mobile else 'desktop', axis=1)

    return df

def filter_data2(df: pd.DataFrame) -> pd.DataFrame:
    df = df[['session_id', 'client_id','Date_time', 'utm_source_new','utm_medium_new',
             'utm_adcontent', 'device_category_new', 'device_brand',  # Выбор значимых атрибутов для анализа данных
             'device_browser','geo_country', 'geo_city']]

    return df


def df_to_dict(df: pd.DataFrame) -> pd.DataFrame:
    dict = df.to_dict('index')

    return dict


def pipeline_ga_sessions(df: pd.DataFrame) -> pd.DataFrame:
    df = filter_data(df)
    df = drop_dublicates(df)
    df = drop_null(df)
    df = refactor(df)
    df = refactor_null(df)
    df = refactor_null2(df)
    df = replace(df)
    df = date_join_time(df)
    df = type_datetime(df)
    df = trafic_result(df)
    df = social_network(df)
    df = mobile_or_desktop(df)
    df = filter_data2(df)
    json_data = df_to_dict(df)

    return json_data

def pipeline_ga_sessions_new(data):
    try:
        connection = pymysql.connect(  # Для подключения к Базе Данных MySQL используется метод pymysql.connect
            host=host,
            port=3306,
            user=user,
            password=password,
            database=db_name,
            cursorclass=pymysql.cursors.DictCursor  # Курсор, возвращающий результаты в виде словаря
        )
        try:
            cursor = connection.cursor()  # Используем метод для получения объекта Cursor из объекта соединения.
            for key, item in data.items():
                session_id = item['session_id']
                client_id = item['client_id']
                Date_time = item['Date_time']
                utm_source_new = item['utm_source_new']
                utm_medium_new = item['utm_medium_new']
                utm_adcontent = item['utm_adcontent']
                device_category_new = item['device_category_new']
                device_brand = item['device_brand']
                device_browser = item['device_browser']
                geo_country = item['geo_country']
                geo_city = item['geo_city']
                try:  # Для выполнения запроса используем метод  curcor.execute()
                    cursor.execute('insert into ga_sessions0(session_id,'
                                   'client_id, Date_time, utm_source_new, utm_medium_new, utm_adcontent,'
                                   'device_category_new, device_brand, device_browser, geo_country,'
                                   'geo_city) value(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                                   (session_id, client_id, Date_time, utm_source_new, utm_medium_new, utm_adcontent,
                                    device_category_new, device_brand, device_browser, geo_country,
                                    geo_city))
                    connection.commit()  # для сохранения запроса используем метод connection.commit()
                    print('Added.')
                except Exception as ex:
                    print('Not added.')
                    continue
        finally:
            connection.close()  # закрываем соединение методом connection.close()
            print('All entries have been added.')
    except Exception as ex:
        print('Connection refused.')
        print(ex)

def pipeline_ga_sessions_new2():
    df = pd.read_csv(f'{path}/data/csv_files/ga_sessions_new.csv', dtype=str)
    data = pipeline_ga_sessions(df)
    pipeline_ga_sessions_new(data)


#pipeline_ga_sessions_new2()
