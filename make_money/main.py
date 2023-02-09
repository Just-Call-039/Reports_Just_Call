# coding: utf-8

import pandas as pd
import pymysql
import telebot

from commons.connect_db import connect_db

token = '5095756650:AAElXGJb5kfvanEXx5FlET6T3HayTjIs_PU'
bot = telebot.TeleBot(token)

host, user, password = connect_db('cloud_128')
connect_mysql = pymysql.Connect(host=host, user=user, passwd=password,
                                db="suitecrm",
                                charset='utf8')

current_dir = r'D:\Отчеты\make_money'
files_dir = rf'{current_dir}\files'
sql_dir = rf'{current_dir}\SQL'
make_money_sql = rf'{sql_dir}\make_money_sql_query.sql'
make_money_file = rf'{files_dir}\make_money_file.csv'

# print(files_dir)
# print(sql_dir)

df = pd.read_sql_query(open(make_money_sql).read(), connect_mysql)
df.to_csv(make_money_file, index=False, sep=';', encoding='utf-8')

bot.send_document(chat_id='-1001412983860', document=open(make_money_file, 'rb'))

# print(df)
print('OK')
