from clickhouse_driver import Client
import pandas as pd

pd.options.mode.chained_assignment = None

client = Client(host='192.168.1.99', port='9000', user='default', password='jdfwl6812hwe',
                database='suitecrm_robot_ch', settings={'use_numpy': True})

print('Чтение файла.')
df = pd.read_csv(r'C:\Users\Supervisor031\Отчеты\Отчет_25\Files\My_request.csv', sep=';', dtype='unicode')
print('Ок.')

# print(df.info())
# print(df.head())

client.insert_dataframe('INSERT INTO suitecrm_robot_ch.request_25 VALUES',
                        df[['phone_number', 'assigned_user_id', 'status_request', 'my_date', 'uniqueid', 'ochered',
                            'project']])
