import pandas as pd
import shutil
import time
import re
import pymysql
from clickhouse_driver import Client


my_connect = pymysql.Connect(host="84.201.164.249", user="glotov", passwd="dZ23HJiNTlf8Jpk4YeafSOHVR2qB65gO",
                             db="suitecrm",
                             charset='utf8')

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
           else cl.user_id_c end          as super
from suitecrm.calls_cstm as cl
         left join suitecrm.calls as cl_1 on cl.id_c = cl_1.id
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


df_all_us = pd.read_sql_query(all_users, my_connect)

df_my_sup = pd.read_sql_query(my_super, my_connect)

df_total_calls = pd.read_sql_query(total_calls, my_connect)

df_calls_31d = pd.read_sql_query(total_calls_31d, my_connect)

to_all_us = r'C:\Users\Supervisor031\Отчеты\Отчет_10\All_users.csv'
to_all_us_copy = r'\\10.88.22.128\dbs\report_10\All_users.csv'
df_all_us.to_csv(to_all_us, index=False, sep=';', encoding='utf-8')
shutil.copyfile(to_all_us, to_all_us_copy)

to_my_sup = r'C:\Users\Supervisor031\Отчеты\Отчет_10\Super.csv'
df_my_sup.to_csv(to_my_sup, index=False, sep=';', encoding='utf-8')

to_total_calls = r'C:\Users\Supervisor031\Отчеты\Отчет_10\Total_calls.csv'
df_total_calls.to_csv(to_total_calls, index=False, sep=';', encoding='utf-8')

to_calls_31d = r'C:\Users\Supervisor031\Отчеты\Отчет_10\Total_calls_31d.csv'
df_calls_31d.to_csv(to_calls_31d, index=False, sep=';', encoding='utf-8')
