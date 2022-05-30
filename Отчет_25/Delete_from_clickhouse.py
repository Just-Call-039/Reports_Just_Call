import time
from clickhouse_driver import Client

start_time = time.time()

with open('my_del.txt', 'a') as file:
    file.write(f'Подключение к БД начато в: {time.strftime("%X")}.\n')
    client = Client(host='192.168.1.99', port='9000', user='default', password='jdfwl6812hwe',
                    database='suitecrm_robot_ch', settings={'use_numpy': True})

    file.write(f'Удаление значений из otchet_25 начато в: {time.strftime("%X")}.\n')
    client.execute("alter table otchet_25 delete where toDate(my_date) between '2022-05-27' and '2022-05-30';")
    file.write(f'Удаление значений из otchet_25 закончено в: {time.strftime("%X")}.\n')

    file.write(f'Удаление значений из request_25 начато в: {time.strftime("%X")}.\n')
    client.execute("alter table request_25 delete where toDate(date_reguest) between '2022-05-27' and '2022-05-30';")
    file.write(f'Удаление значений из request_25 закончено в: {time.strftime("%X")}.\n')

    file.write(f'Конец работы: {time.strftime("%X")}.\n')
    file.write(f'Затрачено времени: {round(time.time() - start_time, 3)} сек.\n')

    file.write('-\n')
