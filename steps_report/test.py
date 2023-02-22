import pandas as pd


def is_number_route(my_str):
    new_list = []
    temp = my_str.strip().split(',')
    for i in temp:
        if i.isdigit():
            new_list.append(int(i))
    return new_list


path = r'D:\test.csv'
new_file = r'D:\test_new.csv'

df = pd.read_csv(path, sep=';')
df.fillna('unknown', inplace=True)
df['route'] = df['route'].apply(is_number_route)

print(df.head())
print(df.info())
print(df.shape)
print(df['route'])

df.to_csv(new_file, sep=';', index=False)
print('-' * 50)
