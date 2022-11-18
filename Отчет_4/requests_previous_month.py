import pandas as pd
import time
import pymysql
import datetime

from commons.convert_time import convert_time
from commons.connect_db import connect_db

print(f'Производится подключение к БД. Начало в: {time.strftime("%X")}.')
now_time = time.time()
host, user, password = connect_db('cloud_117')
my_connect = pymysql.Connect(host=host, user=user, passwd=password,
                             db="suitecrm",
                             charset='utf8')
print(f'Подключение выполнено. Ушло времени: {convert_time(time.time() - now_time)}')
print()

requests_previous_month = open(r'D:\Отчеты\Отчет_4\SQL\requests_previous_month.sql').read()
working_time_previous_month = open(r'D:\Отчеты\Отчет_4\SQL\working_time_previous_month.sql').read()
main_previous_month = open(r'D:\Отчеты\Отчет_4\SQL\main_previous_month.sql').read()

today = datetime.date.today()
year = today.year
month = today.month - 1

print('Запрос из requests_previous_month.sql заносится в ДФ.')
now_time = time.time()
df_st = pd.read_sql_query(requests_previous_month, my_connect)
print(f'Ушло времени: {convert_time(time.time() - now_time)}')
print()

print('ДФ из запроса requests_previous_month.sql записывается в файл.')
now_time = time.time()
to_st = rf'D:\Отчеты\Отчет_4\test\{year}_{month}.csv'
df_st.to_csv(to_st, index=False, sep=';', encoding='utf-8')
print(f'Ушло времени: {convert_time(time.time() - now_time)}')
print()
