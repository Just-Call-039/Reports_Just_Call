import pandas as pd
import time
import pymysql
import telegram_send
from tqdm import tqdm
from clickhouse_driver import Client

tqdm.pandas()


print(f'Производится подключение к БД. Начало в: {time.strftime("%X")}.')
now_time = time.time()
my_connect = pymysql.Connect(host="192.168.1.42", user="glotov", passwd="dZ23HJiNTlf8Jpk4YeafSOHVR2qB65gO",
                             db="suitecrm",
                             charset='utf8')
print(f'Подключение выполнено. Ушло времени: {round(time.time() - now_time, 3)} сек.')
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
select 'RTK'                                                                   project,
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
  and date(date_entered) = date(now()) - interval 1 day
"""

total_calls_one_day = """
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

total_calls_30_day = """
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
where date(call_date) >= date(now()) - interval 30 day;
"""


print('Чтение SQL запросов и занесение в ДФ.')
print()

print('Запрос из status заносится в ДФ.')
now_time = time.time()
df_st = pd.read_sql_query(status, my_connect)
print(f'Ушло времени: {round(time.time() - now_time, 3)} сек.')
print()

print('Запрос из my_request заносится в ДФ.')
now_time = time.time()
df_req = pd.read_sql_query(my_request, my_connect)
print(f'Ушло времени: {round(time.time() - now_time, 3)} сек.')
print()

print('Запрос из total_calls_one_day заносится в ДФ.')
now_time = time.time()
df_calls_one = pd.read_sql_query(total_calls_one_day, my_connect)
print(f'Ушло времени: {round(time.time() - now_time, 3)} сек.')
print()

print('Запрос из total_calls_30_day заносится в ДФ.')
now_time = time.time()
df_calls_30 = pd.read_sql_query(total_calls_30_day, my_connect)
print(f'Ушло времени: {round(time.time() - now_time, 3)} сек.')
print()


print('ДФ из запроса status записывается в файл.')
now_time = time.time()
to_st = r'C:\Users\Supervisor031\Отчеты\Отчет_25\Files\Test_status.csv'
df_st.to_csv(to_st, index=False, sep=';', encoding='utf-8')
print(f'Ушло времени: {round(time.time() - now_time, 3)} сек.')
print()

print('ДФ из запроса my_request записывается в файл.')
now_time = time.time()
to_req = r'C:\Users\Supervisor031\Отчеты\Отчет_25\Files\Test_request.csv'
df_req.to_csv(to_req, index=False, sep=';', encoding='utf-8')
print(f'Ушло времени: {round(time.time() - now_time, 3)} сек.')
print()

print('ДФ из запроса total_calls_one_day записывается в файл.')
now_time = time.time()
to_calls_one = r'C:\Users\Supervisor031\Отчеты\Отчет_25\Files\Test_calls_one.csv'
df_calls_one.to_csv(to_calls_one, index=False, sep=';', encoding='utf-8')
print(f'Ушло времени: {round(time.time() - now_time, 3)} сек.')
print()

print('ДФ из запроса total_calls_30_day записывается в файл.')
now_time = time.time()
to_calls_30 = r'C:\Users\Supervisor031\Отчеты\Отчет_25\Files\Test_calls_30.csv'
df_calls_30.to_csv(to_calls_30, index=False, sep=';', encoding='utf-8')
print(f'Ушло времени: {round(time.time() - now_time, 3)} сек.')
print()
