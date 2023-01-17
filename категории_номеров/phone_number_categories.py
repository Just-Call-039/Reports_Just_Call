import pandas as pd

path_all = r'D:\Отчеты\категории_номеров\\'
file_1 = 'phone_part_1.csv'
file_2 = 'phone_part_2.csv'

file_name = f'{file_1.rstrip(".csv")}_{file_2.rstrip(".csv")}.csv'

with open(f'{path_all}{file_1}', encoding='utf-8', errors='ignore') as file:
    df_1 = pd.read_csv(file, sep=';', encoding='ISO-8859-1')

with open(f'{path_all}{file_2}', encoding='utf-8', errors='ignore') as file:
    df_2 = pd.read_csv(file, sep=';', encoding='ISO-8859-1')

df_test = pd.merge(df_1, df_2, on='phone', how='outer', indicator=True)

df_test.to_csv(f'{path_all}{file_name}')

print(df_test.head())
