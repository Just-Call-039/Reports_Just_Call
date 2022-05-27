import pandas as pd
import shutil
import time
import pymysql
import telegram_send

from connect_db import connect_db
from clear_file import clear_file
from convert_time import convert_time


start_time = time.time()
report = open(r'C:\Users\Supervisor031\Отчеты\Отчет_10\report_10.txt', 'a')
start = '--------------start--------------\n'
end = '---------------end---------------\n'
report.write(start)

print(f'Производится подключение к БД. Начало в: {time.strftime("%X")}.')
telegram_send.send(messages=[f'Начало работы отчета №10 в: {time.strftime("%X")}.'])
report.write(
    f'Производится подключение к БД. Дата: {time.strftime("%d-%m-%Y")}. Время: {time.strftime("%X")}.\n')
now_time = time.time()
host, user, password = connect_db('Maria_db')
my_connect = pymysql.Connect(host=host, user=user, passwd=password,
                             db="suitecrm",
                             charset='utf8')
print(f'Подключение выполнено. Ушло времени: {round(time.time() - now_time, 3)} сек.')
report.write(f'Подключение выполнено. Ушло времени: {round(time.time() - now_time, 3)} сек.\n')
print()

all_users = open(r'C:\Users\Supervisor031\Отчеты\Отчет_10\SQL\All_users.sql').read()

my_super = open(r'C:\Users\Supervisor031\Отчеты\Отчет_10\SQL\Super.sql').read()

total_calls = open(r'C:\Users\Supervisor031\Отчеты\Отчет_10\SQL\Total_calls.sql').read()

total_calls_31d = open(r'C:\Users\Supervisor031\Отчеты\Отчет_10\SQL\Total_calls_31d.sql').read()


try:
    print('Чтение SQL запросов и занесение в ДФ.')
    print()

    print('Запрос из All_users.sql заносится в ДФ.')
    report.write('Запрос из All_users.sql заносится в ДФ.\n')
    now_time = time.time()
    df_all_us = pd.read_sql_query(all_users, my_connect)
    print(f'Ушло времени: {convert_time(time.time() - now_time)}')
    report.write(f'Ушло времени: {convert_time(time.time() - now_time)}\n')
    print()

    print('Запрос из Super.sql заносится в ДФ.')
    report.write('Запрос из Super.sql заносится в ДФ.\n')
    now_time = time.time()
    df_my_sup = pd.read_sql_query(my_super, my_connect)
    print(f'Ушло времени: {convert_time(time.time() - now_time)}')
    report.write(f'Ушло времени: {convert_time(time.time() - now_time)}\n')
    print()

    print('Запрос из Total_calls.sql заносится в ДФ.')
    report.write('Запрос из Total_calls.sql заносится в ДФ.\n')
    now_time = time.time()
    df_total_calls = pd.read_sql_query(total_calls, my_connect)
    print(f'Ушло времени: {convert_time(time.time() - now_time)}')
    report.write(f'Ушло времени: {convert_time(time.time() - now_time)}\n')
    print()

    print('Запрос из Total_calls_31d.sql заносится в ДФ.')
    report.write('Запрос из Total_calls_31d.sql заносится в ДФ.\n')
    now_time = time.time()
    df_calls_31d = pd.read_sql_query(total_calls_31d, my_connect)
    print(f'Ушло времени: {convert_time(time.time() - now_time)}')
    report.write(f'Ушло времени: {convert_time(time.time() - now_time)}\n')
    print()

except:
    print('Произошла ошибка чтения SQL запросов и занесения в ДФ.')
    print()
    telegram_send.send(messages=[f'Произошла ошибка работы отчета №10.'])
    report.write('Произошла ошибка чтения SQL запросов и занесения в ДФ.\n')
    end_time = time.time()
    total_time = end_time - start_time
    # my_min = int(total_time // 60)
    # my_sec = round(total_time % 60, 3)
    print(f'Общее время обработки и создания файлов составило: {convert_time(total_time)}')
    report.write(f'Общее время обработки и создания файлов составило: {convert_time(total_time)}\n')
    report.write(end)
    report.write('\n')
    report.close()

else:
    print('ДФ из запроса All_users.sql записывается в файл.')
    report.write('ДФ из запроса All_users.sql записывается в файл.\n')
    now_time = time.time()
    to_all_us = r'C:\Users\Supervisor031\Отчеты\Отчет_10\Files\All_users.csv'
    to_all_us_copy = r'\\10.88.22.128\dbs\report_10\All_users.csv'
    df_all_us.to_csv(to_all_us, index=False, sep=';', encoding='utf-8')
    print('Копирование файла All_users.csv в сетевое расположение.')
    report.write('Копирование файла All_users.csv в сетевое расположение.\n')
    shutil.copyfile(to_all_us, to_all_us_copy)
    print(f'Ушло времени: {convert_time(time.time() - now_time)}')
    report.write(f'Ушло времени: {convert_time(time.time() - now_time)}\n')
    print()

    print('ДФ из запроса Super.sql записывается в файл.')
    report.write('ДФ из запроса Super.sql записывается в файл.\n')
    now_time = time.time()
    to_my_sup = r'C:\Users\Supervisor031\Отчеты\Отчет_10\Files\Super.csv'
    to_my_sup_copy = r'\\10.88.22.128\dbs\report_10\Super.csv'
    df_my_sup.to_csv(to_my_sup, index=False, sep=';', encoding='utf-8')
    print('Копирование файла Super.csv в сетевое расположение.')
    report.write('Копирование файла Super.csv в сетевое расположение.\n')
    shutil.copyfile(to_my_sup, to_my_sup_copy)
    print(f'Ушло времени: {convert_time(time.time() - now_time)}')
    report.write(f'Ушло времени: {convert_time(time.time() - now_time)}\n')
    print()

    print('ДФ из запроса Total_calls.sql записывается в файл.')
    report.write('ДФ из запроса Total_calls.sql записывается в файл.\n')
    now_time = time.time()
    to_total_calls = r'C:\Users\Supervisor031\Отчеты\Отчет_10\Files\Total_calls.csv'
    to_total_calls_copy = r'\\10.88.22.128\dbs\report_10\Total_calls.csv'
    df_total_calls.to_csv(to_total_calls, index=False, sep=';', encoding='utf-8')
    print('Копирование файла Total_calls.csv в сетевое расположение.')
    report.write('Копирование файла Total_calls.csv в сетевое расположение.\n')
    shutil.copyfile(to_total_calls, to_total_calls_copy)
    print(f'Ушло времени: {convert_time(time.time() - now_time)}')
    report.write(f'Ушло времени: {convert_time(time.time() - now_time)}\n')
    print()

    print('ДФ из запроса Total_calls_31d.sql записывается в файл.')
    report.write('ДФ из запроса Total_calls_31d.sql записывается в файл.\n')
    now_time = time.time()
    to_calls_31d = r'C:\Users\Supervisor031\Отчеты\Отчет_10\Files\Total_calls_31d.csv'
    to_calls_31d_copy = r'\\10.88.22.128\dbs\report_10\Total_calls_31d.csv'
    df_calls_31d.to_csv(to_calls_31d, index=False, sep=';', encoding='utf-8')
    print('Копирование файла Total_calls_31d.csv в сетевое расположение.')
    report.write('Копирование файла Total_calls_31d.csv в сетевое расположение.\n')
    shutil.copyfile(to_calls_31d, to_calls_31d_copy)
    print(f'Ушло времени: {convert_time(time.time() - now_time)}')
    report.write(f'Ушло времени: {convert_time(time.time() - now_time)}\n')
    print()

    print('Обработка пользователей из файла Super.csv.')
    report.write('Обработка пользователей из файла Super.csv.\n')
    clear_file(to_my_sup)
    to_my_sup_clear = r'C:\Users\Supervisor031\Отчеты\Отчет_10\Files\Super_clear.csv'
    to_my_sup_clear_copy = r'\\10.88.22.128\dbs\report_10\Super_clear.csv'
    print('Копирование файла Super_clear.csv в сетевое расположение.')
    report.write('Копирование файла Super_clear.csv в сетевое расположение.\n')
    shutil.copyfile(to_my_sup_clear, to_my_sup_clear_copy)
    print()

    print('Обработка пользователей из файла All_users.csv.')
    report.write('Обработка пользователей из файла All_users.csv.\n')
    clear_file(to_all_us)
    to_all_us_clear = r'C:\Users\Supervisor031\Отчеты\Отчет_10\Files\All_users_clear.csv'
    to_all_us_clear_copy = r'\\10.88.22.128\dbs\report_10\All_users_clear.csv'
    print('Копирование файла Super_clear.csv в сетевое расположение.')
    report.write('Копирование файла Super_clear.csv в сетевое расположение.\n')
    shutil.copyfile(to_all_us_clear, to_all_us_clear_copy)
    print()

    end_time = time.time()
    total_time = end_time - start_time
    # my_min = int(total_time // 60)
    # my_sec = round(total_time % 60, 3)
    print(f'Общее время обработки и создания файлов составило: {convert_time(total_time)}')
    telegram_send.send(messages=[f'Отчет №10 выполнен.\n'
                                 f'Общее время работы составило: {convert_time(total_time)}'])
    report.write(f'Общее время обработки и создания файлов составило: {convert_time(total_time)}\n')
    report.write(end)
    report.write('\n')
    report.close()
