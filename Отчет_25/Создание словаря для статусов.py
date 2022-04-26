import time


# Функция для очистки значения от лишних символов (пробелы, кавычки).
def my_c(i):
    i = i.strip().strip("''").strip('""')
    return i


# Функция для отображения статуса. Необходимо передать очередь и последний шаг.
def my_status(ochered, key):
    for now in status_dict[ochered].keys():
        if key in now.split(','):
            return status[status_dict[ochered][now]]


status_dict = {}
# Мертвые шаги.
dead_steps = ['0', '1', '111', '261', '262']
# Исходный файл с очередями и статусами.
my_file = input('Введите название и расширение файла на чтение: ')

start_time = time.time()

# Открытие исходного файла.
with open(my_file, encoding='utf-8') as file:
    # Итерация по строкам.
    for now in file:
        # Разделение строки по ";".
        now = [my_c(i) for i in now.split(';')]

        # Если первое значение - слово, передаем статусы в список.
        if now[0].isalpha():
            status = [my_c(i) for i in now]
            continue

        # Далее записываем словарь с ключом - очередь, значениями - последний шаг,
        # к последнему шагу - код (индекс статуса из списка со статусами).
        status_dict[now[0]] = {now[i]: i for i in range(1, len(status))}

to_file = 'Status_dict.csv'
print(f'Создан файл {to_file}.')

# Открытие файла на запись.
with open(to_file, 'w', encoding='utf-8') as to_file:
    to_file.write('ochered;last_step;status\n')
    # Итерация по ключам (очередям).
    for now in status_dict:
        # Итерация по спискам шагов.
        for step in status_dict[now].keys():
            # Выделение одного последнего шага из списка.
            for last_step in step.split(','):
                # Запись в файл. Очередь, последний шаг, статус.
                to_file.write(f'{now};{last_step};{my_status(now, last_step)}\n')

print(f'Время обработки и создания словаря: {round(time.time() - start_time, 3)} сек.')
