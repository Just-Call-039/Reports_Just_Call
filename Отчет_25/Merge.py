import pandas as pd
import time
from tqdm import tqdm

tqdm.pandas()


# Функция для проверки технической возможности.
# Необходимо передать очередь, was_rep, шаги.
def teh_v(o, was_rep, route):
    # Если was_rep = 0, то статус "Не проверялось".
    if was_rep == 0:
        return "Didn't check"
    # Если was_rep = 1, то идем дальше.
    else:
        route = route.split(',')
        o = str(o)
        # Если очередь присутствует в одном из словарей, начинаем проверку.
        if o in tuple(est_tehv.keys()) or o in tuple(net_tehv.keys()):
            for i in route:
                # Проверка, находится ли шаг в словаре "Есть техническая возможность".
                # Если находится, то возвращает значение 1.
                if i in est_tehv[o]:
                    value = 1
                    return value
                # Проверка, находится ли шаг в словаре "Нет технической возможности".
                # Если находится, то возвращает значение 0.
                elif i in net_tehv[o]:
                    value = 0
                    return value
            # Если ничего не найдено в словарях, возвращает "Неизвестно".
            else:
                return "Don't know"
        # Если ничего не найдено в словарях, возвращает "Неизвестно".
        else:
            return "Don't know"


start_time = time.time()

print(f'Создание словаря с технической возможностью начато в: {time.strftime("%X")}.')
now_time = time.time()
# Создание словаря "Нет технической возможности".
net_tehv = dict()
# Создание словаря "Есть техническая возможность".
est_tehv = dict()

# Открытие файла со статусами.
# Находим статусы "Нет технической возможности" или "Есть техническая возможность".
# Записываем в соответсвующий словарь пары ключ - очередь, а для него значение - конкретный шаг.
with open(r'C:\Users\Supervisor031\Отчеты\Отчет_25\Files\Status_dict.csv') as status:
    for now in status:
        now = now.split(';')
        o, step, stat = now[0], now[1], now[2]
        if stat == 'net_teh_vozmozhnosti':
            if o in net_tehv:
                net_tehv[o].add(step)
            else:
                net_tehv[o] = set()
                net_tehv[o].add(step)
        elif stat == 'est_teh_vozmozhnost':
            if o in est_tehv:
                est_tehv[o].add(step)
            else:
                est_tehv[o] = set()
                est_tehv[o].add(step)

print(f'Ушло времени: {round(time.time() - now_time, 3)} сек.')
print()

print(f'Открытие файлов начато в: {time.strftime("%X")}.')
now_time = time.time()

# Файл БД из запроса "Total_calls.sql".
# l_f = input('Введите название и расширение файла БД на чтение: ')
l_f = r'C:\Users\Supervisor031\Отчеты\Отчет_25\Files\Total_calls.csv'

# Файл со статусами из "Создание словаря для статусов".
# r_f = input('Введите название и расширение файла со статусами на чтение: ')
r_f = r'C:\Users\Supervisor031\Отчеты\Отчет_25\Files\Status_dict.csv'

print('Открытие файла БД из запроса "Total_calls.sql".')
# Открытие файла БД из запроса "Total_calls.sql".
# left = pd.read_csv(l_f, sep=';', nrows=1000000)
left = pd.read_csv(l_f, sep=';')

print('Открытие файла с регионами.')
# Открытие файла с регионами.
city = pd.read_excel(r'C:\Users\Supervisor031\Отчеты\Отчет_25\Files\Макрорегионы.xlsx')

print('Открытие файла со статусами.')
# Открытие файла со статусами.
right = pd.read_csv(r_f, sep=';')
print(f'Ушло времени: {round(time.time() - now_time, 3)} сек.')
print()

print(f'Создание нового столбца "Техническая возможность" начато в: {time.strftime("%X")}.')
print()
# Создание нового столбца "Техническая возможность".
# Значение определяется с помощью функции для проверки технической возможности.
left['teh_vozmozhnost'] = left.progress_apply(lambda x: teh_v(x['ochered'], x['was_repeat'], x['route']), axis=1)
print()

print(f'Слияние таблиц и преобразование столбцов начато в: {time.strftime("%X")}.')
now_time = time.time()

print('Слияние двух таблиц. БД из запроса "Total_calls.sql" и файла с регионами.')
# Слияние двух таблиц. БД из запроса "Total_calls.sql" и файла с регионами.
left = pd.merge(left, city[['city_c', 'Область']], left_on='city_c', right_on='city_c', how='left')

print(
    'Слияние модифицированной БД из запроса "Total_calls.sql" и файла со статусами из "Создание словаря для статусов".')
# Дальнейшее слияние таблиц. Модифицированная БД и файл со статусами из "Создание словаря для статусов".
result = pd.merge(left, right, left_on=['ochered', 'last_step'], right_on=['ochered', 'last_step'], how='left')
# result = pd.DataFrame(result, columns = my_columns)
print('Преобразование столбцов.')
# Заменяем все пустые значения из столбца "alive" на 1.
result['alive'] = result['alive'].fillna(1)
# Преобразуем значения из столбца "alive" в "int64:.
result = result.astype({'alive': 'int64'})
# Переименовал столбец "Область" в "region".
result.rename(columns={'Область': 'region'}, inplace=True)
print(f'Ушло времени: {round(time.time() - now_time, 3)} сек.')
print()

# Вывод информации об итоговом ДФ.
result.info()
print()

print(f'Запись итогового ДФ в файл начата в: {time.strftime("%X")}.')
now_time = time.time()
# Запись ДФ в файл.
to_file = r'C:\Users\Supervisor031\Отчеты\Отчет_25\Files\F1_result.csv'
result.to_csv(to_file, index=False, sep=';', encoding='utf-8')
print(f'Создан файл {to_file} в {time.strftime("%X")}.')
print(f'Ушло времени: {round(time.time() - now_time, 3)} сек.')
print()

end_time = time.time()
total_time = end_time - start_time
my_min = int(total_time // 60)
my_sec = round(total_time % 60, 3)
print(f'Общее время обработки и создания файлов: {my_min} мин., {my_sec} сек.')
