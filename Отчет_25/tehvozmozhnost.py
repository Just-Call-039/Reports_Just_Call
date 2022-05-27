# Функция для проверки технической возможности.
# Необходимо передать очередь, шаги.


def teh_v(ochered, route):
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
            o, step, stat = now[0].strip(), now[1].strip(), now[2].strip()
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

    net_tv_step = ['105', '106', '107']
    route = tuple(i.strip() for i in str(route).split(','))
    ochered = str(ochered)
    # Если последний шаг в списке шагов с НТВ, то возвращаем 0.
    if route[-1] in net_tv_step:
        t_v = 0
        return t_v
    # Если последний шаг не в списке шагов с НТВ, то начинаем перебор шагов.
    elif route[-1] not in net_tv_step:
        for st in route:
            # Если очередь в словаре ЕТВ и найден соответствующий шаг для такой очереди, то возвращаем 1.
            if ochered in tuple(est_tehv.keys()) and st in est_tehv[ochered]:
                t_v = 1
                return t_v
            # Если очередь в словаре НТВ и найден соответствующий шаг для такой очереди, то возвращаем 0.
            elif ochered in tuple(net_tehv.keys()) and st in net_tehv[ochered]:
                t_v = 0
                return t_v
        # Если никакие условия не выполнены, то возвращаем "Didn't check".
        else:
            t_v = "Didn't check"
            return t_v
