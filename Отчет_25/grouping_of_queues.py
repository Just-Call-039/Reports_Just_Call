from __future__ import print_function
from googleapiclient.discovery import build
from google.oauth2 import service_account
from clickhouse_driver import Client
from commons.connect_db import connect_db
import datetime
import pandas as pd

# Взято из официальной документации.
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

# Ключ - json файл. Формируется в аккаунте гугла.
json_file = r'D:\Отчеты\not_share\bionic-client-381907-8eb5ac3d5272.json'

credentials = service_account.Credentials.from_service_account_file(json_file, scopes=SCOPES)
spreadsheet_service = build('sheets', 'v4', credentials=credentials)
drive_service = build('drive', 'v3', credentials=credentials)

# ИД таблицы - берется из адреса.
SAMPLE_SPREADSHEET_ID = '1AJN3WDVuSu_y1eEss7Q7rT2a0Zyk2azc0GmOp7I9HQc'
# Необходимые колонки из таблицы.
SAMPLE_RANGE_NAME = 'A:C'

sheet = spreadsheet_service.spreadsheets()
result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                            range=SAMPLE_RANGE_NAME).execute()
values = result.get('values', [])

# print(result)
# print(*[i for i in values], sep='\n')

today = datetime.date.today()

# Создаем датафрейм с данными из таблицы.
df = pd.DataFrame(values[1:], index=[i for i in range(len(values) - 1)],
                  columns=['queue', 'group', 'project'])

# Добавляем текущую дату в датафрейм.
df['date_add'] = today

# print(df)

pd.options.mode.chained_assignment = None

host, user, password = connect_db('Click')
client = Client(host=host, port='9000', user=user, password=password,
                database='suitecrm_robot_ch', settings={'use_numpy': True})

# Отправка датафрейма в БД.
client.insert_dataframe('INSERT INTO suitecrm_robot_ch.grouping_of_queues VALUES',
                        df[['date_add', 'queue', 'group', 'project']])
