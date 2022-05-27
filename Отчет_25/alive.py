# Функция для определения типа звонка.


def alive(route):
    # Мертвые шаги.
    dead_steps = ['0', '1', '111', '261', '262']
    # Если последний шаг в списке мертвых, то для звонка записываем 0.
    if str(route).split(',')[-1] in dead_steps:
        return 0
    # Иначе для звонка записываем 1.
    else:
        return 1
