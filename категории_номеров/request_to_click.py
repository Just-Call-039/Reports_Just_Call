import pandas as pd

path = r'D:\Отчеты\категории_номеров\all_requests.csv'

# with open(path, encoding='utf-8', errors='ignore') as file:
#     df = pd.read_csv(file, sep=';', encoding='latin-1')

df = pd.read_csv(path, sep=';', encoding='latin-1', low_memory=False)

print(df.head())
