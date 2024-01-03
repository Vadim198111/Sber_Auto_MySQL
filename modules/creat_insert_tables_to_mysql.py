import pymysql
from config import host,user,password,db_name
from modules.create_tables_to_sql import creat_tables
from modules.ga_hits_to_sql import pipeline_ga_hits_new
from modules.ga_sessions_to_sql import pipeline_ga_sessions_new


def creat_insert_to_tables():
    try:
        connection = pymysql.connect(  # Для подключения к Базе Данных MySQL используется метод pymysql.connect
            host=host,
            port=3306,
            user=user,
            password=password,
            database=db_name,
            cursorclass=pymysql.cursors.DictCursor  # Курсор, возвращающий результаты в виде словаря
        )
        print('ok!!!')
        try:

            with connection.cursor() as cursor:  # Используем метод для получения объекта Cursor из объекта соединения.
                count_table1 = cursor.execute("SELECT * FROM information_schema.tables WHERE table_name = 'ga_hits0'")
                print(count_table1)
                count_table2 = cursor.execute("SELECT * FROM information_schema.tables WHERE table_name = 'ga_sessions0'")
                print(count_table1)
                if count_table1 == 0 and count_table2 == 0:
                    creat_tables()
                    pipeline_ga_hits_new()
                    pipeline_ga_sessions_new()

        finally:
            connection.close()  # закрываем соединение методом connection.close()
            print('Connection close.')
    except Exception as ex:
        print('Connection refused.')
        print(ex)

creat_insert_to_tables()