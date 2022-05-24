import pandas as pd
import time
import pymysql
import telegram_send
from tqdm import tqdm
from clickhouse_driver import Client


tqdm.pandas()


# Функция для очистки значения от лишних символов (пробелы, кавычки).
def my_c(i):
    i = i.strip().strip("''").strip('""')
    return i


# Функция для отображения статуса. Необходимо передать очередь и последний шаг.
def my_status(ochered, key):
    for now in status_dict[ochered].keys():
        if key in now.split(','):
            return status[status_dict[ochered][now]]


# Функция для проверки технической возможности.
# Необходимо передать очередь, шаги.
def teh_v(ochered, route):
    net_tv_step = ['105', '106', '107']
    route = tuple(i.strip() for i in str(route).split(','))
    ochered = str(ochered)
    # Если последний шаг в списке шагов с НТВ, то возвращаем 0.
    if route[-1] in net_tv_step:
        t_v = 0
        return t_v
    # Если последний шаг не в списке шагов с НТВ, то начинаем перебор шагов.
    elif route[-1] not in net_tv_step:
        for st in route:
            # Если очередь в словаре ЕТВ и найден соответствующий шаг для такой очереди, то возвращаем 1.
            if ochered in tuple(est_tehv.keys()) and st in est_tehv[ochered]:
                t_v = 1
                return t_v
            # Если очередь в словаре НТВ и найден соответствующий шаг для такой очереди, то возвращаем 0.
            elif ochered in tuple(net_tehv.keys()) and st in net_tehv[ochered]:
                t_v = 0
                return t_v
        # Если никакие условия не выполнены, то возвращаем "Didn't check".
        else:
            t_v = "Didn't check"
            return t_v


# Функция для определения типа звонка.
def alive(route):
    # Мертвые шаги.
    dead_steps = ['0', '1', '111', '261', '262']
    # Если последний шаг в списке мертвых, то для звонка записываем 0.
    if str(route).split(',')[-1] in dead_steps:
        return 0
    # Иначе для звонка записываем 1.
    else:
        return 1


start_time = time.time()
report = open(r'C:\Users\Supervisor031\Отчеты\Отчет_25\report_25.txt', 'a')
start = '--------------start--------------\n'
end = '---------------end---------------\n'
report.write(start)

print(f'Производится подключение к БД. Начало в: {time.strftime("%X")}.')
telegram_send.send(messages=[f'Начало работы отчета №25 в: {time.strftime("%X")}.'])
report.write(
    f'Производится подключение к БД. Дата: {time.strftime("%d-%m-%Y")}. Время: {time.strftime("%X")}.\n')
now_time = time.time()
my_connect = pymysql.Connect(host="84.201.164.249", user="glotov", passwd="dZ23HJiNTlf8Jpk4YeafSOHVR2qB65gO",
                             db="suitecrm",
                             charset='utf8')
print(f'Подключение выполнено. Ушло времени: {round(time.time() - now_time, 3)} сек.')
report.write(f'Подключение выполнено. Ушло времени: {round(time.time() - now_time, 3)} сек.\n')
print()

status = """
select substring(turn, 11, 4) as ochered,
       steps_autoanswer       as avtootvetchik,
       steps_transferred      as perevod,
       steps_refusing         as otkaz,
       reset_greet            as sbros_na_privetsvii,
       x_ptv                  as net_teh_vozmozhnosti,
       have_ptv               as est_teh_vozmozhnost,
       reset_pres             as sbros_na_presentacii,
       is_subs                as yavlyaetsya_abonentom,
       steps_inconvenient     as neudobno_govorit,
       steps_error            as oshobka_razgovora
from jc_robot_reportconfig
where deleted = 0;
"""

my_request = """
select phone_number,
       assigned_user_id,
       status as status_request,
       date_reguest,
       uniqueid,
       ochered,
       project
from (select my_phone_work                                                as phone_number,
             assigned_user_id,
             status,
             reguest.date                                                 as date_reguest,
             new_rob.uniqueid,
             new_rob.ochered,
             new_rob.phone,
             row_number() over (partition by phone order by my_date desc) as num,
             project
      from (select 'RTK'                                                                   project,
                   concat(8, right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                   date_entered + interval 2 hour                                       as date,
                   assigned_user_id,
                   status
            from suitecrm.jc_meetings_rostelecom
            where status != 'Error'
              and date(date_entered) = date(now()) - interval 1 day
            union all
            select 'Beeline'                                                               project,
                   concat(8, right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                   date_entered + interval 2 hour                                       as date,
                   assigned_user_id,
                   status
            from suitecrm.jc_meetings_beeline
            where status != 'Error'
              and date(date_entered) = date(now()) - interval 1 day
            union all
            select project,
                   concat(8, right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                   date_entered + interval 2 hour                                       as date,
                   assigned_user_id,
                   status
            from suitecrm.jc_meetings_domru
            where status != 'Error'
              and date(date_entered) = date(now()) - interval 1 day
            union all
            select project,
                   concat(8, right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                   date_entered + interval 2 hour                                       as date,
                   assigned_user_id,
                   status
            from suitecrm.jc_meetings_ttk
            where status != 'Error'
              and date(date_entered) = date(now()) - interval 1 day
            union all
            select 'NBN'                                                                   project,
                   concat(8, right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                   date_entered + interval 2 hour                                       as date,
                   assigned_user_id,
                   status
            from suitecrm.jc_meetings_netbynet
            where status != 'Error'
              and date(date_entered) = date(now()) - interval 1 day
            union all
            select project,
                   concat(8, right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                   date_entered + interval 2 hour                                       as date,
                   assigned_user_id,
                   status
            from suitecrm.jc_meetings_mts jc_meetings_mts
            where status != 'Error'
              and date(date_entered) = date(now()) - interval 1 day
            union all
            select project,
                   concat(8, right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                   date_entered + interval 2 hour                                       as date,
                   assigned_user_id,
                   status
            from suitecrm.jc_meetings_beeline_mnp
            where status != 'Error'
              and date(date_entered) = date(now()) - interval 1 day) as reguest
               left join
           (select call_date + interval 2 hour as my_date,
                   uniqueid,
                   substring(dialog, 11, 4)    as ochered,
                   phone
            from suitecrm_robot.jc_robot_log
            where date(call_date) >= date(now()) - interval 90 day) as new_rob
           on reguest.my_phone_work = new_rob.phone) as total
where num = 1;
"""

total_calls = """
select call_date + interval 2 hour as my_date,
       uniqueid,
       substring(dialog, 11, 4)    as ochered,
       last_step,
       route,
       billsec,
       client_status,
       otkaz,
       directory,
       server_number,
       city_c,
       ptv_c,
       marker,
       was_repeat,
       phone
from suitecrm_robot.jc_robot_log
where date(call_date) = date(now()) - interval 1 day;
"""


try:
    print('Чтение SQL запросов и занесение в ДФ.')
    print()

    print('Запрос из Status.sql заносится в ДФ.')
    report.write('Запрос из Status.sql заносится в ДФ.\n')
    now_time = time.time()
    df_st = pd.read_sql_query(status, my_connect)
    print(f'Ушло времени: {round(time.time() - now_time, 3)} сек.')
    report.write(f'Ушло времени: {round(time.time() - now_time, 3)} сек.\n')
    print()

    print('Запрос из My_request.sql заносится в ДФ.')
    report.write('Запрос из My_request.sql заносится в ДФ.\n')
    now_time = time.time()
    df_req = pd.read_sql_query(my_request, my_connect)
    print(f'Ушло времени: {round(time.time() - now_time, 3)} сек.')
    report.write(f'Ушло времени: {round(time.time() - now_time, 3)} сек.\n')
    print()

    print('Запрос из Total_calls.sql заносится в ДФ.')
    report.write('Запрос из Total_calls.sql заносится в ДФ.\n')
    now_time = time.time()
    df_calls = pd.read_sql_query(total_calls, my_connect)
    print(f'Ушло времени: {round(time.time() - now_time, 3)} сек.')
    report.write(f'Ушло времени: {round(time.time() - now_time, 3)} сек.\n')
    print()

except:
    print('Произошла ошибка чтения SQL запросов и занесения в ДФ.')
    print()
    telegram_send.send(messages=[f'Произошла ошибка работы отчета №25.'])
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
    print('ДФ из запроса Status.sql записывается в файл.')
    report.write('ДФ из запроса Status.sql записывается в файл.\n')
    now_time = time.time()
    to_st = r'C:\Users\Supervisor031\Отчеты\Отчет_25\Files\Status.csv'
    df_st.to_csv(to_st, index=False, sep=';', encoding='utf-8')
    print(f'Ушло времени: {round(time.time() - now_time, 3)} сек.')
    report.write(f'Ушло времени: {round(time.time() - now_time, 3)} сек.\n')
    print()

    print('ДФ из запроса My_request.sql записывается в файл.')
    report.write('ДФ из запроса My_request.sql записывается в файл.\n')
    now_time = time.time()
    to_req = r'C:\Users\Supervisor031\Отчеты\Отчет_25\Files\My_request.csv'
    df_req.to_csv(to_req, index=False, sep=';', encoding='utf-8')
    print(f'Ушло времени: {round(time.time() - now_time, 3)} сек.')
    report.write(f'Ушло времени: {round(time.time() - now_time, 3)} сек.\n')
    print()

    print('ДФ из запроса Total_calls.sql записывается в файл.')
    report.write('ДФ из запроса Total_calls.sql записывается в файл.\n')
    now_time = time.time()
    to_calls = r'C:\Users\Supervisor031\Отчеты\Отчет_25\Files\Total_calls.csv'
    df_calls.to_csv(to_calls, index=False, sep=';', encoding='utf-8')
    print(f'Ушло времени: {round(time.time() - now_time, 3)} сек.')
    report.write(f'Ушло времени: {round(time.time() - now_time, 3)} сек.\n')
    print()


    status_time = time.time()
    to_file = r'C:\Users\Supervisor031\Отчеты\Отчет_25\Files\Status_dict.csv'
    print(f'Создан файл со статусами {to_file}.')
    print('Производится запись статусов в файл.')
    status_dict = {}

    # Исходный файл с очередями и статусами Status.csv.
    # Открытие исходного файла.
    with open(to_st, encoding='utf-8') as file:
        # Итерация по строкам.
        for now in file:
            # Разделение строки по ";".
            now = [my_c(i) for i in now.split(';')]
            # Если первое значение - слово, передаем статусы в список.
            if now[0].isalpha():
                status = [my_c(i) for i in now]
                continue
            # Далее записываем словарь с ключом - очередь, значениями - последний шаг,
            # к последнему шагу - код (индекс статуса из списка со статусами).
            status_dict[now[0]] = {now[i]: i for i in range(1, len(status))}

    # Открытие файла на запись.
    with open(to_file, 'w', encoding='utf-8') as to_file:
        to_file.write('ochered;last_step;status\n')
        # Итерация по ключам (очередям).
        for now in status_dict:
            # Итерация по спискам шагов.
            for step in status_dict[now].keys():
                # Выделение одного последнего шага из списка.
                for last_step in step.split(','):
                    # Запись в файл. Очередь, последний шаг, статус.
                    to_file.write(f'{now};{last_step};{my_status(now, last_step)}\n')

    print(f'Время обработки, создания и записи файла составило: {round(time.time() - status_time, 3)} сек.')
    print()


    print(f'Создание словаря с технической возможностью начато в: {time.strftime("%X")}.')
    now_time = time.time()
    # Создание словаря "Нет технической возможности".
    net_tehv = dict()
    # Создание словаря "Есть техническая возможность".
    est_tehv = dict()

    # Открытие файла со статусами.
    # Находим статусы "Нет технической возможности" или "Есть техническая возможность".
    # Записываем в соответсвующий словарь пары ключ - очередь, а для него значение - конкретный шаг.
    with open(r'C:\Users\Supervisor031\Отчеты\Отчет_25\Files\Status_dict.csv') as status:
        for now in status:
            now = now.split(';')
            o, step, stat = now[0].strip(), now[1].strip(), now[2].strip()
            if stat == 'net_teh_vozmozhnosti':
                if o in net_tehv:
                    net_tehv[o].add(step)
                else:
                    net_tehv[o] = set()
                    net_tehv[o].add(step)
            elif stat == 'est_teh_vozmozhnost':
                if o in est_tehv:
                    est_tehv[o].add(step)
                else:
                    est_tehv[o] = set()
                    est_tehv[o].add(step)

    print(f'Ушло времени: {round(time.time() - now_time, 3)} сек.')
    print()

    print(f'Открытие файлов начато в: {time.strftime("%X")}.')
    report.write(f'Открытие файлов начато в: {time.strftime("%X")}.\n')
    now_time = time.time()

    # Файл БД из запроса "Total_calls.sql".
    # l_f = input('Введите название и расширение файла БД на чтение: ')
    l_f = r'C:\Users\Supervisor031\Отчеты\Отчет_25\Files\Total_calls.csv'

    # Файл со статусами из "Создание словаря для статусов".
    # r_f = input('Введите название и расширение файла со статусами на чтение: ')
    r_f = r'C:\Users\Supervisor031\Отчеты\Отчет_25\Files\Status_dict.csv'

    print('Открытие файла БД из запроса "Total_calls.sql".')
    # Открытие файла БД из запроса "Total_calls.sql".
    # left = pd.read_csv(l_f, sep=';', nrows=1000000)
    left = pd.read_csv(l_f, sep=';')

    print('Открытие файла с регионами.')
    # Открытие файла с регионами.
    city = pd.read_excel(r'C:\Users\Supervisor031\Отчеты\Отчет_25\Files\Макрорегионы.xlsx')

    print('Открытие файла со статусами.')
    # Открытие файла со статусами.
    right = pd.read_csv(r_f, sep=';')
    print(f'Ушло времени: {round(time.time() - now_time, 3)} сек.')
    report.write(f'Ушло времени: {round(time.time() - now_time, 3)} сек.\n')
    print()

    print(f'Создание нового столбца "Техническая возможность" начато в: {time.strftime("%X")}.')
    print()
    # Создание нового столбца "Техническая возможность".
    # Значение определяется с помощью функции для проверки технической возможности.
    left['teh_vozmozhnost'] = left.progress_apply(lambda x: teh_v(x['ochered'], x['route']), axis=1)
    print()

    print(f'Слияние таблиц и преобразование столбцов начато в: {time.strftime("%X")}.')
    now_time = time.time()

    print('Слияние двух таблиц. БД из запроса "Total_calls.sql" и файла с регионами.')
    # Слияние двух таблиц. БД из запроса "Total_calls.sql" и файла с регионами.
    left = pd.merge(left, city[['city_c', 'Область']], left_on='city_c', right_on='city_c', how='left')

    print('Слияние модифицированной БД из запроса "Total_calls.sql" и файла со статусами '
          'из "Создание словаря для статусов".')
    # Дальнейшее слияние таблиц. Модифицированная БД и файл со статусами из "Создание словаря для статусов".
    result = pd.merge(left, right, left_on=['ochered', 'last_step'], right_on=['ochered', 'last_step'], how='left')
    # result = pd.DataFrame(result, columns = my_columns)
    print('Преобразование столбцов.')
    print(f'Создание нового столбца "alive" начато в: {time.strftime("%X")}.')
    print()
    # Создание нового столбца "alive".
    # Значение определяется с помощью функции для определения типа звонка.
    result['alive'] = result.progress_apply(lambda x: alive(x['route']), axis=1)
    print()
    # Преобразуем значения из столбца "alive" в "int64:.
    result = result.astype({'alive': 'int64'})
    # Переименовал столбец "Область" в "region".
    result.rename(columns={'Область': 'region'}, inplace=True)
    print(f'На слияние таблиц ушло времени: {round(time.time() - now_time, 3)} сек.')
    print()

    # Вывод информации об итоговом ДФ.
    result.info()
    print()

    print(f'Запись итогового ДФ в файл начата в: {time.strftime("%X")}.')
    report.write(f'Запись итогового ДФ в файл начата в: {time.strftime("%X")}.\n')
    now_time = time.time()
    # Запись ДФ в файл.
    to_file = r'C:\Users\Supervisor031\Отчеты\Отчет_25\Files\F_result.csv'
    result.to_csv(to_file, index=False, sep=';', encoding='utf-8')
    print(f'Создан файл {to_file} в: {time.strftime("%X")}.')
    report.write(f'Создан файл {to_file} в: {time.strftime("%X")}.\n')
    print(f'Ушло времени: {round(time.time() - now_time, 3)} сек.')
    report.write(f'Ушло времени: {round(time.time() - now_time, 3)} сек.\n')
    print()


    pd.options.mode.chained_assignment = None

    client = Client(host='192.168.1.99', port='9000', user='default', password='jdfwl6812hwe',
                    database='suitecrm_robot_ch', settings={'use_numpy': True})

    print(f'Чтение файла F_result.csv начато в: {time.strftime("%X")}.')
    report.write(f'Чтение файла F_result.csv начато в: {time.strftime("%X")}.\n')
    now_time = time.time()
    df = pd.read_csv(to_file, sep=';', dtype='unicode')
    print('Файл прочитан.')
    print('Производится запись файла в БД.')
    client.insert_dataframe('INSERT INTO suitecrm_robot_ch.otchet_25 VALUES',
                            df[['my_date', 'uniqueid', 'ochered', 'last_step', 'route', 'billsec', 'client_status', 'otkaz',
                                'directory',
                                'server_number', 'city_c', 'ptv_c', 'marker', 'was_repeat', 'phone', 'teh_vozmozhnost',
                                'region',
                                'status', 'alive']])
    print(f'Ушло времени: {round(time.time() - now_time, 3)} сек.')
    report.write(f'Файл занесен в БД. Ушло времени: {round(time.time() - now_time, 3)} сек.\n')
    print()


    print(f'Чтение файла My_request.csv начато в: {time.strftime("%X")}.')
    report.write(f'Чтение файла My_request.csv начато в: {time.strftime("%X")}.\n')
    now_time = time.time()
    df = pd.read_csv(to_req, sep=';', dtype='unicode')
    print('Файл прочитан.')
    print('Производится запись файла в БД.')
    client.insert_dataframe('INSERT INTO suitecrm_robot_ch.request_25 VALUES',
                            df[['phone_number', 'assigned_user_id', 'status_request', 'date_reguest', 'uniqueid',
                                'ochered', 'project']])
    print(f'Ушло времени: {round(time.time() - now_time, 3)} сек.')
    report.write(f'Файл занесен в БД. Ушло времени: {round(time.time() - now_time, 3)} сек.\n')
    print()

    end_time = time.time()
    total_time = end_time - start_time
    my_min = int(total_time // 60)
    my_sec = round(total_time % 60, 3)
    print(f'Общее время обработки и создания файлов составило: {my_min} мин., {my_sec} сек.')
    telegram_send.send(messages=[f'Отчет №25 выполнен.\n'
                                 f'Общее время работы составило: {my_min} мин., {my_sec} сек.'])
    report.write(f'Общее время обработки и создания файлов составило: {my_min} мин., {my_sec} сек.\n')
    report.write(end)
    report.write('\n')
    report.close()
