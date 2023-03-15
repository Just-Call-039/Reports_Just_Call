import pandas as pd

from tqdm import tqdm

tqdm.pandas()


def is_number_route(my_str):
    new_list = []
    temp = my_str.strip().split(',')
    for i in temp:
        if i.isdigit():
            new_list.append(int(i))
    return new_list


def medium_step(my_str):
    if len(my_str) in (0, 1):
        return 'empty'
    else:
        return my_str[:-1]


def reset_step(my_str):
    my_str = my_str.strip().split(',')
    if len(my_str) == 0:
        return 'empty'
    else:
        return my_str[-1]


files_from_sql = r'D:\Отчеты\steps_report\files\files_from_sql'
main_folder = r'D:\Отчеты\steps_report\files\main_folder'
uniqueid_medium_folder = r'D:\Отчеты\steps_report\files\uniqueid_medium_folder'

name = '2023_3_14.csv'

main_calls_raw = rf'{files_from_sql}\{name}'
main_calls_true = rf'{main_folder}\{name}'
medium_step_file = rf'{uniqueid_medium_folder}\{name}'

df = pd.read_csv(main_calls_raw, sep=';', encoding='utf-8')
df.fillna('unknown', inplace=True)
df['reset_step'] = df['route'].progress_apply(reset_step)

df_uniqueid = pd.DataFrame()
df_uniqueid['uniqueid'] = df['uniqueid']
df_uniqueid['route'] = df['route'].progress_apply(is_number_route)
df_uniqueid['medium_step'] = df_uniqueid['route'].progress_apply(medium_step)
df_uniqueid = df_uniqueid.explode('medium_step')

# df['group_uniq_id'] = df.groupby(['uniqueid']).cumcount() + 1
# df_new = df.explode('route')

df.to_csv(main_calls_true, sep=';', index=False, encoding='utf-8')
df_uniqueid.to_csv(medium_step_file, sep=';', index=False, encoding='utf-8')

print(df.head())
print(df.info())
print(df.shape)

print('-' * 50)

print(df_uniqueid.head())
