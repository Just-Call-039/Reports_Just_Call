import pandas as pd
import time
import pymysql
import telegram_send
from tqdm import tqdm
from clickhouse_driver import Client

tqdm.pandas()

start_time = time.time()

pd.options.mode.chained_assignment = None

client = Client(host='192.168.1.99', port='9000', user='default', password='jdfwl6812hwe',
                database='suitecrm_robot_ch', settings={'use_numpy': True})

print(f'Чтение файла F_result.csv начато в: {time.strftime("%X")}.')
now_time = time.time()
df = pd.read_csv(r'C:\Users\Supervisor031\Отчеты\Отчет_25\Files\F_result.csv', sep=';', dtype='unicode')
print('Файл прочитан.')
print('Производится запись файла в БД.')
client.insert_dataframe('INSERT INTO suitecrm_robot_ch.otchet_25 VALUES',
                        df[['my_date', 'uniqueid', 'ochered', 'last_step', 'route', 'billsec', 'client_status', 'otkaz',
                            'directory',
                            'server_number', 'city_c', 'ptv_c', 'marker', 'was_repeat', 'phone', 'teh_vozmozhnost',
                            'region',
                            'status', 'alive']])
print(f'Ушло времени: {round(time.time() - now_time, 3)} сек.')
print()

end_time = time.time()
total_time = end_time - start_time
my_min = int(total_time // 60)
my_sec = round(total_time % 60, 3)
print(f'Общее время обработки и создания файлов составило: {my_min} мин., {my_sec} сек.')
telegram_send.send(messages=[f'Отчет №25 выполнен.\n'
                             f'Общее время работы составило: {my_min} мин., {my_sec} сек.'])
