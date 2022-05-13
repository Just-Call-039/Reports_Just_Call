import pandas as pd
import shutil
import time
import re
import pymysql
import telegram_send


# Функция обработки пользователей.
def clear_file(my_file):
    start_clear = time.time()
    step = 0
    to_file = my_file.replace('.csv', '_clear.csv')
    print(f"Создан файл: {to_file}.")

    # Открытие файла на запись.
    with open(to_file, 'w', encoding='utf-8') as to_file:
        # Открытие файла на чтение.
        with open(my_file, encoding='utf-8') as file:
            for now in file:

                step += 1
                # if step == 100:
                #     break

                # Пробегаем по каждой строке. Делим по ",".
                now = now.strip().split(';')
                # Записываем заголовок.
                if now[0] == 'id':
                    to_file.write('id;first_name;last_name;group;department_c;status\n')
                    continue
                # ИД.
                my_id = now[0]
                # Имя.
                first = now[1]
                # Фамилия.
                last = now[2]
                # Отдел.
                dep = now[3].strip().strip('""')
                # Группа.
                group = []
                # print(my_id, first, last, dep)

                # Если "я" стоит в начале имени, сотрудник уволен.
                if re.search(r'^я', first):
                    # Статус - уволен.
                    status = 'dismissed'
                    # Делим строку с именем по пробелу и "_".
                    first = re.split(r'[ _]', first)
                    # Пробегаем по каждому слову в имени.
                    for word in first:
                        # И по каждой букве в слове.
                        for now in word:
                            # Если слово начинается с "я", просто пропускаем.
                            if now == 'я':
                                continue
                            # Если буква - это цифра, добавляем к группе сотрудника.
                            elif now.isdigit():
                                group.append(now)
                            # Если буква - заглавная, то это начало имени.
                            elif now.isupper():
                                # Ищем позицию заглавной буквы в слове. Делаем срез по строке.
                                k = word.find(now)
                                name = word[k:]
                                break
                    # Преобразую группу из списка в строку.
                    group = ''.join(group)
                    # Если в строке с именем не было группы, то такой сотрудник записывается соответствующим образом.
                    if group == '':
                        group = 'unknown_group'
                # Если "я" нет в начале имени, сотрудник работает.
                else:
                    # Статус - работает.
                    status = 'working'
                    # Делим строку с именем по пробелу и "_".
                    first = re.split(r'[ _]', first)
                    # Если длина строки == 1, в строке содержится только имя. Группа отсутствует.
                    if len(first) == 1:
                        name = first[0].strip()
                        group = 'unknown_group'
                    # Иначе, извлекаем имя и группу.
                    else:
                        name = first[1]
                        group = first[0]

                # Проверка значений на пустые данные.
                if my_id is None or my_id == '' or my_id == ' ':
                    my_id = 'unknown_id'
                if name is None or name == '' or name == ' ':
                    name = 'unknown_name'
                if dep is None or dep == '' or dep == ' ':
                    dep = 'unknown_department'
                if status is None or status == '' or status == ' ':
                    status = 'unknown_status'

                # to_file.write('id,first_name,last_name,group,department_c,status')
                to_file.write(f'{my_id};{name};{last};{group};{dep};{status}\n')
                # print(my_id, name, last, group, dep, status)
    print(f"Время обработки {step} строк составило: {round(time.time() - start_clear, 3)} сек.")


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
my_connect = pymysql.Connect(host="84.201.164.249", user="glotov", passwd="dZ23HJiNTlf8Jpk4YeafSOHVR2qB65gO",
                             db="suitecrm",
                             charset='utf8')
print(f'Подключение выполнено. Ушло времени: {round(time.time() - now_time, 3)} сек.')
report.write(f'Подключение выполнено. Ушло времени: {round(time.time() - now_time, 3)} сек.\n')
print()

all_users = """
select id, first_name, last_name, department_c
from suitecrm.users
         left join suitecrm.users_cstm on users.id = users_cstm.id_c;
"""

my_super = """
select id, first_name, last_name, department_c
from suitecrm.users
         left join suitecrm.users_cstm on users.id = users_cstm.id_c
where id in (select distinct supervisor from suitecrm.worktime_supervisor);
"""

total_calls = """
select case
           when cl_1.assigned_user_id is null or cl_1.assigned_user_id = '' or cl_1.assigned_user_id = ' '
               then 'unknown_id'
           else cl_1.assigned_user_id end as user,
       case
           when cl.queue_c is null or cl.queue_c = '' or cl.queue_c = ' ' then 'unknown_queue'
           else cl.queue_c end            as queue,
       cl.otkaz_c                         as ref,
       date(cl_1.date_entered)            as calls_date,
       case
           when cl.user_id_c is null or cl.user_id_c = '' or cl.user_id_c = ' ' then 'unknown_id'
           else cl.user_id_c end          as super,
       case
           when city_c is null then concat(town_c, 'OBL')
           when city_c in ('', ' ') then concat(town_c, 'OBL')
           else city_c
           end                            as city
from suitecrm.calls_cstm as cl
         left join suitecrm.calls as cl_1 on cl.id_c = cl_1.id
         left join suitecrm.contacts on cl.asterisk_caller_id_c = contacts.phone_work
         left join suitecrm.contacts_cstm on contacts_cstm.id_c = contacts.id
where cl.result_call_c = 'refusing'
  and cl_1.date_entered >= '2022-01-01';
"""

total_calls_31d = """
select user,
       queue,
       ref,
       result_call_c,
       calls_date,
       super
from (
         select case
                    when cl_1.assigned_user_id is null or cl_1.assigned_user_id = '' or cl_1.assigned_user_id = ' '
                        then 'unknown_id'
                    else cl_1.assigned_user_id end                                                  as user,
                case
                    when cl.queue_c is null or cl.queue_c = '' or cl.queue_c = ' ' then 'unknown_queue'
                    else cl.queue_c end                                                             as queue,
                cl.otkaz_c                                                                          as ref,
                date(cl_1.date_entered)                                                             as calls_date,
                date_entered,
                case
                    when cl.user_id_c is null or cl.user_id_c = '' or cl.user_id_c = ' ' then 'unknown_id'
                    else cl.user_id_c end                                                           as super,
                result_call_c,
                cl.asterisk_caller_id_c,
                row_number() over (partition by cl.asterisk_caller_id_c order by date_entered desc) as rwn
         from suitecrm.calls_cstm as cl
                  left join suitecrm.calls as cl_1 on cl.id_c = cl_1.id
         where cl_1.date_entered >= date(now()) - interval 31 day
         # and user_id_c is not null
     ) as t1
where rwn = 1;
"""


try:
    print('Чтение SQL запросов и занесение в ДФ.')
    print()

    print('Запрос из All_users.sql заносится в ДФ.')
    report.write('Запрос из All_users.sql заносится в ДФ.\n')
    now_time = time.time()
    df_all_us = pd.read_sql_query(all_users, my_connect)
    print(f'Ушло времени: {round(time.time() - now_time, 3)} сек.')
    report.write(f'Ушло времени: {round(time.time() - now_time, 3)} сек.\n')
    print()

    print('Запрос из Super.sql заносится в ДФ.')
    report.write('Запрос из Super.sql заносится в ДФ.\n')
    now_time = time.time()
    df_my_sup = pd.read_sql_query(my_super, my_connect)
    print(f'Ушло времени: {round(time.time() - now_time, 3)} сек.')
    report.write(f'Ушло времени: {round(time.time() - now_time, 3)} сек.\n')
    print()

    print('Запрос из Total_calls.sql заносится в ДФ.')
    report.write('Запрос из Total_calls.sql заносится в ДФ.\n')
    now_time = time.time()
    df_total_calls = pd.read_sql_query(total_calls, my_connect)
    print(f'Ушло времени: {round(time.time() - now_time, 3)} сек.')
    report.write(f'Ушло времени: {round(time.time() - now_time, 3)} сек.\n')
    print()

    print('Запрос из Total_calls_31d.sql заносится в ДФ.')
    report.write('Запрос из Total_calls_31d.sql заносится в ДФ.\n')
    now_time = time.time()
    df_calls_31d = pd.read_sql_query(total_calls_31d, my_connect)
    print(f'Ушло времени: {round(time.time() - now_time, 3)} сек.')
    report.write(f'Ушло времени: {round(time.time() - now_time, 3)} сек.\n')
    print()

except:
    print('Произошла ошибка чтения SQL запросов и занесения в ДФ.')
    print()
    telegram_send.send(messages=[f'Произошла ошибка работы отчета №10.'])
    report.write('Произошла ошибка чтения SQL запросов и занесения в ДФ.\n')
    end_time = time.time()
    total_time = end_time - start_time
    my_min = int(total_time // 60)
    my_sec = round(total_time % 60, 3)
    print(f'Общее время обработки и создания файлов составило: {my_min} мин., {my_sec} сек.')
    report.write(f'Общее время обработки и создания файлов составило: {my_min} мин., {my_sec} сек.\n')
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
    print(f'Ушло времени: {round(time.time() - now_time, 3)} сек.')
    report.write(f'Ушло времени: {round(time.time() - now_time, 3)} сек.\n')
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
    print(f'Ушло времени: {round(time.time() - now_time, 3)} сек.')
    report.write(f'Ушло времени: {round(time.time() - now_time, 3)} сек.\n')
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
    print(f'Ушло времени: {round(time.time() - now_time, 3)} сек.')
    report.write(f'Ушло времени: {round(time.time() - now_time, 3)} сек.\n')
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
    print(f'Ушло времени: {round(time.time() - now_time, 3)} сек.')
    report.write(f'Ушло времени: {round(time.time() - now_time, 3)} сек.\n')
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
    my_min = int(total_time // 60)
    my_sec = round(total_time % 60, 3)
    print(f'Общее время обработки и создания файлов составило: {my_min} мин., {my_sec} сек.')
    telegram_send.send(messages=[f'Отчет №10 выполнен.\n'
                                 f'Общее время работы составило: {my_min} мин., {my_sec} сек.'])
    report.write(f'Общее время обработки и создания файлов составило: {my_min} мин., {my_sec} сек.\n')
    report.write(end)
    report.write('\n')
    report.close()
