import pandas as pd
import time
import pymysql
import telegram_send
from tqdm import tqdm
from clickhouse_driver import Client

tqdm.pandas()

t_calls = pd.read_csv('Total_calls.csv', sep=';', dtype='unicode')
t_req = pd.read_csv('Total_req.csv', sep=';', dtype='unicode')

agg_func_selection = {'my_date': ['last']}
new_calls = t_calls.sort_values(by=['my_date'], ascending=True).groupby(['phone']).agg(
    agg_func_selection).reset_index()
new_calls.columns = new_calls.columns.map(' '.join)
new_calls.rename(columns={'phone ': 'phone', 'my_date last': 'last'}, inplace=True)
new_calls.to_csv('New_calls.csv', index=False, sep=';', encoding='utf-8')
new_calls = pd.read_csv('New_calls.csv', sep=';', dtype='unicode')

# clear_calls = pd.merge(new_calls, t_calls, left_on=['phone', ('my_date', 'last')], right_on=['phone', 'my_date'],
#                        how='left')
clear_calls = pd.merge(new_calls, t_calls, left_on=['phone', 'last'], right_on=['phone', 'my_date'],
                       how='left')
clear_calls.to_csv('clear_calls.csv', index=False, sep=';', encoding='utf-8')

day_req = pd.merge(t_req, clear_calls[['phone', 'uniqueid', 'ochered']], left_on='my_phone_work', right_on='phone',
                   how='inner')
del day_req['phone']
day_req = day_req.reindex(
    columns=['my_phone_work', 'assigned_user_id', 'status', 'date', 'uniqueid', 'ochered', 'project'])
day_req.rename(columns={'my_phone_work': 'phone_number', 'status': 'status_request', 'date': 'date_reguest'},
               inplace=True)
day_req.to_csv('day_req.csv', index=False, sep=';', encoding='utf-8')
