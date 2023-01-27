import pandas as pd
import time
import pymysql

from tqdm import tqdm
from clickhouse_driver import Client
from calendar import monthrange

tqdm.pandas()

pd.options.mode.chained_assignment = None

start_time = time.time()

path = r'D:\Отчеты\категории_номеров\files\\'

client_click = Client(host='192.168.1.99', port='9000', user='default', password='jdfwl6812hwe',
                      database='suitecrm_robot_ch', settings={'use_numpy': True})

for req_year in range(2017, 2024):
    for req_month in range(1, 13):
        time_1 = time.time()

        calls_month = (req_month - 3) % 12 + 1
        if calls_month > req_month:
            calls_year = req_year - 1
        else:
            calls_year = req_year
        #         print(req_month, req_year, calls_month, calls_year)

        sql = f"""with temp_robot as (select *
                    from (select toDate(call_date)                                                      as call_date,
                                 uniqueid,
                                 substring(dialog, 11, 4)                                               as ochered,
                                 phone,
                                 row_number() over (partition by phone order by toDate(call_date) desc) as num
                          from suitecrm_robot_ch.jc_robot_log
                          where toDate(call_date) between toDate('{req_year}-{req_month}-01') - interval 2 month
                                    and toDate('{req_year}-{req_month}-01') + interval 1 month - interval 1 day) as temp
                    where temp.num = 1),
                     temp_requests as (select *
                                       from suitecrm_robot_ch.all_requests
                                       where toMonth(request_date) = {req_month}
                                         and toYear(request_date) = {req_year})

                select temp_requests.request_date,
                       temp_requests.project,
                       temp_requests.phone_request,
                       temp_requests.user,
                       temp_requests.super,
                       temp_requests.status,
                       temp_robot.uniqueid
                from temp_requests
                         left join temp_robot on temp_requests.phone_request = temp_robot.phone;
            """

        df = pd.DataFrame(client_click.query_dataframe(sql))

        df.fillna('unknown', inplace=True)
        df.replace(r'\N', 'unknown', inplace=True)

        df_to_file = f'{req_year}_{req_month}.csv'
        full_path = f'{path}{df_to_file}'
        df.to_csv(full_path, index=False, sep=';', encoding='utf-8')

        client_click.insert_dataframe('INSERT INTO suitecrm_robot_ch.all_requests_id VALUES',
                                      df[['request_date', 'project', 'phone_request', 'user', 'super',
                                          'status', 'uniqueid']])

        time_2 = time.time()
        #         print(sql)
        print(df.head())
        print(f'Обработан: {df_to_file}')
        print(f'Ушло времени: {round(time_2 - time_1, 3)} сек.')
        print('-' * 70)

end_time = time.time()
print()
print(f'Ушло времени на все файлы: {round(end_time - start_time, 3)} сек.')
