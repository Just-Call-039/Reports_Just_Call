import pandas as pd
import time
import pymysql
from tqdm import tqdm
from clickhouse_driver import Client


tqdm.pandas()


start_time = time.time()

print(f'Производится подключение к БД. Начало в: {time.strftime("%X")}.')
now_time = time.time()
my_connect = pymysql.Connect(host="84.201.164.249", user="glotov", passwd="dZ23HJiNTlf8Jpk4YeafSOHVR2qB65gO",
                             db="suitecrm",
                             charset='utf8')
print(f'Подключение выполнено. Ушло времени: {round(time.time() - now_time, 3)} сек.')
print()

for day in range(1, 28):
    print(f'Текущая дата: 2022-04-{day}')
    print()

    start_day = time.time()

    my_request = f"""
    select distinct phone_number,
                assigned_user_id,
                status as status_request,
                date_reguest,
                uniqueid,
                ochered,
                project
from (select my_phone_work as phone_number,
             assigned_user_id,
             status,
             reguest.date  as date_reguest,
             my_date       as calls_date,
             new_rob.uniqueid,
             new_rob.ochered,
             new_rob.phone,
             project
      from (select 'RTK'                                                                   project,
                   concat(8, right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                   date_entered + interval 2 hour                                       as date,
                   assigned_user_id,
                   status
            from suitecrm.jc_meetings_rostelecom
            where status != 'Error'
              and date(date_entered) = '2022-04-{day}'
            union all
            select 'Beeline'                                                               project,
                   concat(8, right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                   date_entered + interval 2 hour                                       as date,
                   assigned_user_id,
                   status
            from suitecrm.jc_meetings_beeline
            where status != 'Error'
              and date(date_entered) = '2022-04-{day}'
            union all
            select project,
                   concat(8, right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                   date_entered + interval 2 hour                                       as date,
                   assigned_user_id,
                   status
            from suitecrm.jc_meetings_domru
            where status != 'Error'
              and date(date_entered) = '2022-04-{day}'
            union all
            select project,
                   concat(8, right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                   date_entered + interval 2 hour                                       as date,
                   assigned_user_id,
                   status
            from suitecrm.jc_meetings_ttk
            where status != 'Error'
              and date(date_entered) = '2022-04-{day}'
            union all
            select 'NBN'                                                                   project,
                   concat(8, right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                   date_entered + interval 2 hour                                       as date,
                   assigned_user_id,
                   status
            from suitecrm.jc_meetings_netbynet
            where status != 'Error'
              and date(date_entered) = '2022-04-{day}'
            union all
            select project,
                   concat(8, right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                   date_entered + interval 2 hour                                       as date,
                   assigned_user_id,
                   status
            from suitecrm.jc_meetings_mts jc_meetings_mts
            where status != 'Error'
              and date(date_entered) = '2022-04-{day}'
            union all
            select project,
                   concat(8, right(replace(replace(phone_work, ' ', ''), '-', ''), 10)) as my_phone_work,
                   date_entered + interval 2 hour                                       as date,
                   assigned_user_id,
                   status
            from suitecrm.jc_meetings_beeline_mnp
            where status != 'Error'
              and date(date_entered) = '2022-04-{day}') as reguest
               left join
           (select call_date + interval 2 hour as my_date,
                   uniqueid                    as uniqueid,
                   substring(dialog, 11, 4)    as ochered,
                   phone
            from suitecrm_robot.jc_robot_log as jrl
            where date(call_date) =
                  (select max(date(call_date)) from suitecrm_robot.jc_robot_log as jrl2 where jrl.phone = jrl2.phone)
              and phone in (select phone_work
                            from suitecrm.jc_meetings_rostelecom
                            where status != 'Error'
                              and date(date_entered) = '2022-04-{day}'
                            union all
                            select phone_work
                            from suitecrm.jc_meetings_beeline
                            where status != 'Error'
                              and date(date_entered) = '2022-04-{day}'
                            union all
                            select jc_meetings_domru.phone_work
                            from suitecrm.jc_meetings_domru
                            where status != 'Error'
                              and date(date_entered) = '2022-04-{day}'
                            union all
                            select jc_meetings_ttk.phone_work
                            from suitecrm.jc_meetings_ttk
                            where status != 'Error'
                              and date(date_entered) = '2022-04-{day}'
                            union all
                            select jc_meetings_netbynet.phone_work
                            from suitecrm.jc_meetings_netbynet
                            where status != 'Error'
                              and date(date_entered) = '2022-04-{day}'
                            union all
                            select jc_meetings_mts.phone_work
                            from suitecrm.jc_meetings_mts jc_meetings_mts
                            where status != 'Error'
                              and date(date_entered) = '2022-04-{day}'
                            union all
                            select jc_meetings_beeline_mnp.phone_work
                            from suitecrm.jc_meetings_beeline_mnp
                            where status != 'Error'
                              and date(date_entered) = '2022-04-{day}')
              and date(call_date) <= '2022-04-{day}'
            group by phone) as new_rob
           on reguest.my_phone_work = new_rob.phone) as total;
    """

    print('Запрос из My_request.sql заносится в ДФ.')
    now_time = time.time()
    df_req = pd.read_sql_query(my_request, my_connect)
    print(f'Ушло времени: {round(time.time() - now_time, 3)} сек.')
    print()

    print(df_req)
    print()

    print('ДФ из запроса My_request.sql записывается в файл.')
    now_time = time.time()
    to_req = r'C:\Users\Supervisor031\Отчеты\Отчет_25\Files\My_request.csv'
    df_req.to_csv(to_req, index=False, sep=';', encoding='utf-8')
    print(f'Ушло времени: {round(time.time() - now_time, 3)} сек.')
    print()

    pd.options.mode.chained_assignment = None

    client = Client(host='192.168.1.99', port='9000', user='default', password='jdfwl6812hwe',
                    database='suitecrm_robot_ch', settings={'use_numpy': True})

    print(f'Чтение файла My_request.csv начато в: {time.strftime("%X")}.')
    now_time = time.time()
    df = pd.read_csv(to_req, sep=';', dtype='unicode')
    print('Файл прочитан.')
    print('Производится запись файла в БД.')
    client.insert_dataframe('INSERT INTO suitecrm_robot_ch.request_25 VALUES',
                            df[['phone_number', 'assigned_user_id', 'status_request', 'date_reguest', 'uniqueid',
                                'ochered',
                                'project']])
    print(f'Ушло времени: {round(time.time() - now_time, 3)} сек.')
    print()

    end_day = time.time()
    total_day = end_day - start_day
    my_min_day = int(total_day // 60)
    my_sec_day = round(total_day % 60, 3)
    print(f'Общее время обработки одного дня составило: {my_min_day} мин., {my_sec_day} сек.')
    print()

end_time = time.time()
total_time = end_time - start_time
my_min = int(total_time // 60)
my_sec = round(total_time % 60, 3)
print(f'Общее время обработки и создания файлов составило: {my_min} мин., {my_sec} сек.')
