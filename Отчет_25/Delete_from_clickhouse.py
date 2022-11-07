import time
from clickhouse_driver import Client
from commons.connect_db import connect_db

start_time = time.time()

with open('my_del.txt', 'a') as file:
    file.write(f'Подключение к БД начато в: {time.strftime("%X")}.\n')
    host, user, password = connect_db('Click')
    client = Client(host=host, port='9000', user=user, password=password,
                    database='suitecrm_robot_ch', settings={'use_numpy': True})

    file.write(f'Удаление значений из otchet_25 начато в: {time.strftime("%X")}.\n')
    client.execute("alter table otchet_25 delete where toDate(my_date) between '2022-11-04' and '2022-11-06';")
    file.write(f'Удаление значений из otchet_25 закончено в: {time.strftime("%X")}.\n')

    file.write(f'Удаление значений из request_25 начато в: {time.strftime("%X")}.\n')
    client.execute("alter table request_25 delete where toDate(date_reguest) between '2022-11-04' and '2022-11-06';")
    file.write(f'Удаление значений из request_25 закончено в: {time.strftime("%X")}.\n')

    file.write(f'Конец работы: {time.strftime("%X")}.\n')
    file.write(f'Затрачено времени: {round(time.time() - start_time, 3)} сек.\n')

    file.write('-\n')
