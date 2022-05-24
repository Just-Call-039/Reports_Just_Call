import pandas as pd
import time
import pymysql
from sqlalchemy import create_engine
import telegram_send
from tqdm import tqdm
from clickhouse_driver import Client


tqdm.pandas()


my_connect = create_engine("mysql+pymysql://glotov:dZ23HJiNTlf8Jpk4YeafSOHVR2qB65gO@84.201.164.249")
my_connect.connect()
# my_connect = pymysql.Connect(host="84.201.164.249", user="glotov", passwd="dZ23HJiNTlf8Jpk4YeafSOHVR2qB65gO",
#                              db="suitecrm",
#                              charset='utf8')

sql_file = open(r'C:\Users\Supervisor031\Отчеты\Отчет_25\SQL\Total_calls_yesterday.sql').read()
df_calls = pd.read_sql_query(sql_file, my_connect)
df_calls.to_csv('Test_13.csv', sep=';', index=False)

print(df_calls)
