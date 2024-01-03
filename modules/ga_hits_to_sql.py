import os
import pandas as pd
import pymysql
from config import host,user,password,db_name

# Укажем путь к файлам проекта:
# -> $PROJECT_PATH при запуске в Airflow
# -> иначе - текущая директория при локальном запуске
path = os.environ.get('PROJECT_PATH', '..')
#df = pd.read_csv(f'{path}/data/csv_files/ga_hits_new.csv',dtype=str)

def filter_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df[[
        'session_id',
        'hit_page_path',  # Выбор значимых атрибутов для анализа данных
        'event_action'
    ]]
    return df


def drop_dublicates(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates()   # Удаляем дубликаты в таблице

    return df


def event_action_new(df: pd.DataFrame) -> pd.DataFrame:
    event_result = ['sub_car_claim_click', 'sub_car_claim_submit_click', # Целевое действие — события типа
                    'sub_open_dialog_click', 'sub_custom_question_submit_click',# «Оставить заявку» и «Заказать звонок»
                    'sub_call_number_click', 'sub_callback_submit_click', 'sub_submit_success',
                    'sub_car_request_submit_click']

    df['event_action_new'] = df.apply(lambda x: 1 if x.event_action in event_result else 0, axis=1)
    # Создаём новую колонку с двумя значениями из визита : 0 и 1(целевое значение)
    return df


def filter_data2(df: pd.DataFrame) -> pd.DataFrame:
    df = df[['session_id','hit_page_path','event_action_new']]  # Выбор значимых атрибутов для анализа данных

    return df


def drop_dublicates2(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates() # Удаляем дубликаты в таблице

    return df


def new(df: pd.DataFrame) -> pd.DataFrame:
    df['new'] = df.hit_page_path.apply(lambda x: x.lower().split('/')[1]) # Делим строку и оставляем подстрочку
                                                               # где располагается 'cars' с последующими подстрочками
    return df                                                  # с информацией про марку и модель автомобиля

def tables(df: pd.DataFrame) -> pd.DataFrame:
    df5 = df[(df['new'] == 'cars')]  # выводим датасет со значением 'cars'
    df6 = df[(df['new'] != 'cars')]  # выводим датасет со значениями кроме 'cars'
    df7 = df5[(df5['event_action_new'] == 1)]  # выводим датасет со значением 'cars' с целевым действием в конверсию
    df8 = df5[(df5['event_action_new'] == 0)]  # выводим датасет со значением 'cars' без целевого действия в конверсию
    df7 = df7.copy()
    # считаем колличество подстрочек,чтобы впоследствии определить расположение информации про марку и модель автомобиля
    df7['count_count'] = df7.hit_page_path.apply(lambda x: len(x.lower().split('/')))
    df9 = df7[(df7['count_count'] == 6)]  # датасет,где располагается информация про марку и модель автомобиля
    df10 = df7[(df7['count_count'] != 6)]  # датасет,где модель и марка автомобиля не выявляется системным подходом
    df9 = df9.copy()
    # новая колонка с маркой и моделью автомобиля
    df9['new_auto'] = df9.hit_page_path.apply(lambda x: (x.lower().split('/')[3]) + ':' + (x.lower().split('/')[4]))
    df = pd.concat([df6, df8, df9, df10]) # соединяем датафрейм с новыми признаками

    return df


def filter_data3(df: pd.DataFrame) -> pd.DataFrame:
    df = df[['session_id', # Выбор значимых атрибутов для анализа данных
               'new',
               'event_action_new',
               'new_auto']]

    return df


def new_page(df: pd.DataFrame) -> pd.DataFrame:
    # создаём колонку 'new_page',которая указывает посещение страниц с автомобилями, либо нет(все остальные страницы)
    df['new_page'] = df.apply(lambda x: 'cars' if x.new == 'cars' else 'pages', axis=1)

    return df

def filter_data4(df: pd.DataFrame) -> pd.DataFrame:
    df = df[['session_id',
               'new_page',  # Выбор значимых атрибутов для анализа данных
               'event_action_new',
               'new_auto']]

    return df

def drop_dublicates3(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates()  # Удаление дублирующихся строк

    return df

def tables2(df: pd.DataFrame) -> pd.DataFrame:
    # df15 выводим датасет,чтобы соблюдалось условие : НЕ событие в конверсию И НЕ пустое значение марки и модели авто
    df15 = df[~(df['event_action_new'] == 1 & df['new_auto'].isna())]
    # df16 Удаляем дубликаты и остаётся одна запись с маркой и моделью авто и записи с пустыми значением марки и модели
    df16 = df15.drop_duplicates(['session_id', 'new_page', 'event_action_new'])
    df17 = df16[df16['event_action_new'] == 1] # Создаём датасет с событием в конверсию
    # df18 Создаём датасет без события в конверсию и посещение страниц с автомобилями
    df18 = df16[(df16['event_action_new'] == 0) & (df16['new_page'] == 'cars')]
    df19 = df16.drop_duplicates('session_id', keep=False)  # Стратегия: удаляем полностью ВСЕ дубликаты
    df20 = pd.concat([df19, df17, df18]) # Соединяем датасеты
    df21 = df20.drop_duplicates()  # удаляем дубликаты
    df22 = df21.drop_duplicates('session_id', keep=False) # Сново полное удаление всех дубликатов:
    df23 = pd.concat([df22, df17]) # Возвращаемся к датасету,содержащим полезную информацию про автомобили при событии
                                                                  # в конверсию.Присоединяем его к очищенному датасету
    df = df23.drop_duplicates()  # удаляем дубликаты и получаем датасет с минимальной потерей информации
             # Принятая стратегия стримилась сохранить полную информацию для анализа ВОРОНКИ посещения сайта!!!
    return df

def refactor2(df: pd.DataFrame) -> pd.DataFrame:
    df['new_auto'] = df['new_auto'].fillna('(none)') # Обрабатываем пропуски, заполнив незаполненные
                                                    # значения значением (none), и сохраните изменения
    return df


def replace(df: pd.DataFrame) -> pd.DataFrame:
    df['session_id'] = df.session_id.apply(lambda x: x.replace('.', '')) # Убираем точки в записях колонки

    return df

def df_to_dict(df: pd.DataFrame) -> pd.DataFrame:
    dict = df.to_dict('index')

    return dict

def pipeline_ga_hits(df: pd.DataFrame) -> pd.DataFrame:
    df = filter_data(df)
    df = drop_dublicates(df)
    df = event_action_new(df)
    df = filter_data2(df)
    df = drop_dublicates2(df)
    df = new(df)
    df = tables(df)
    df = filter_data3(df)
    df = new_page(df)
    df = filter_data4(df)
    df = drop_dublicates3(df)
    df = tables2(df)
    df = refactor2(df)
    df = replace(df)
    json_data = df_to_dict(df)

    return json_data


def pipeline_ga_hits_new(data) -> None:
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
            cursor = connection.cursor()  # метод для получения объекта Cursor из объекта соединения.
            for key, item in data.items():
                session_id = item['session_id']
                new_page = item['new_page']
                event_action_new = item['event_action_new']
                new_auto = item['new_auto']
                try:
                    cursor.execute('insert into ga_hits0 (session_id,'  # Для выполнения запроса используем
                                   'new_page,'  # метод  curcor.execute()
                                   'event_action_new,'
                                   'new_auto) value(%s, %s, %s, %s)',
                                   (session_id, new_page, event_action_new, new_auto))
                    connection.commit()  # для сохранения запроса используем метод connection.commit()
                    print('Added.')
                except Exception as ex:
                    print('Not added.')
                    continue
        finally:
            connection.close()  # закрываем соединение методом connection.close()
            print('All entries have been added!')
    except Exception as ex:
        print('Connection refused.')
        print(ex)

def pipeline_ga_hits_new2():
    df = pd.read_csv(f'{path}/data/csv_files/ga_hits_new.csv', dtype=str)
    data = pipeline_ga_hits(df)
    pipeline_ga_hits_new(data)



#pipeline_ga_hits_new2()
