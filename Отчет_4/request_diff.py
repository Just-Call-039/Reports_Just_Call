import pandas as pd

df_old = pd.read_csv(r'C:\Users\Supervisor031\requests_current_month.csv', sep=';')
df_new = pd.read_csv(r'C:\Users\Supervisor031\Result_14.csv', sep=';')

print(df_old.head())
print(df_new.head())

now = df_old.merge(df_new, on='my_phone_work', how='outer', suffixes=['', '_'], indicator=True)
print(now[now['_merge'] != 'both'])
