import pymysql
from config import host,user,password,db_name


def creat_tables():
    try:
        connection = pymysql.connect(  # Для подключения к Базе Данных MySQL используется метод pymysql.connect
            host=host,
            port=3306,
            user=user,
            password=password,
            database=db_name,
            cursorclass=pymysql.cursors.DictCursor  # Курсор, возвращающий результаты в виде словаря
        )
        print('ok')
        try:
            with connection.cursor() as cursor:  # Используем метод для получения объекта Cursor из объекта соединения.
                # Создаём таблицу ga_hits0 с первичным ключём session_id
                create_table_query = ('CREATE TABLE IF NOT EXISTS ga_hits0 (session_id VARCHAR(64)'
                                      ',new_page VARCHAR(32)'
                                      ',event_action_new VARCHAR(32) '
                                      ',new_auto VARCHAR(32)'
                                      ',PRIMARY KEY (session_id));')
                cursor.execute(create_table_query)  # Для выполнения запроса используем метод  curcor.execute()
                print('Table ga_hits0 crated succes!')
            with connection.cursor() as cursor:  # Используем метод для получения объекта Cursor из объекта соединения.
                # Создаём таблицу ga_sessions0 с внешним ключём session_id и ссылаемся на таблицу ga_hits0
                create_table_query = ('CREATE TABLE IF NOT EXISTS ga_sessions0 (session_id VARCHAR(64)'
                                      ',client_id VARCHAR(32)'
                                      ',Date_time DATETIME,utm_source_new VARCHAR(32)'
                                      ',utm_medium_new VARCHAR(32)'
                                      ',utm_adcontent VARCHAR(32)'
                                      ',device_category_new VARCHAR(32)'
                                      ',device_brand VARCHAR(32)'
                                      ',device_browser VARCHAR(32)'
                                      ',geo_country VARCHAR(32)'
                                      ',geo_city VARCHAR(32),FOREIGN KEY (session_id)'
                                      'REFERENCES ga_hits0(session_id));')
                cursor.execute(create_table_query)  # Для выполнения запроса используем метод  curcor.execute()
                print('Table ga_sessions0 crated succes!')
        finally:
            connection.close()  # закрываем соединение методом connection.close()
    except Exception as ex:
        print('Connection refused.')
        print(ex)


#creat_tables()


