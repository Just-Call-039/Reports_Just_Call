import pandas as pd


def is_number(my_str):
    new_list = []
    temp = my_str.strip().split(',')
    for i in temp:
        if i.isdigit():
            new_list.append(i)
    return new_list


path = r'D:\test.csv'
new_file = r'D:\test_new.csv'

df = pd.read_csv(path, sep=';')
df.fillna('unknown', inplace=True)
df['new_route'] = df['route'].apply(is_number)

# print(str(df['route']).split(','))
print(df.info())
print(df.shape)
print(df['route'])

# df.to_csv(new_file, index=False)
print('-' * 50)
print(df['new_route'])

# for i in df['route']:
#     print(str(i.strip().split(',')))
