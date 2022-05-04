import time
from clickhouse_driver import Client

start_time = time.time()

with open('my_del.txt', 'a') as file:
    file.write(f'Подключение к БД начато в: {time.strftime("%X")}.\n')
    client = Client(host='192.168.1.99', port='9000', user='default', password='jdfwl6812hwe',
                    database='suitecrm_robot_ch', settings={'use_numpy': True})

    file.write(f'Удаление значений из otchet_25 начато в: {time.strftime("%X")}.\n')
    client.execute("alter table otchet_25 delete where my_date between '2022-04-21' and '2022-04-27';")
    file.write(f'Удаление значений из otchet_25 закончено в: {time.strftime("%X")}.\n')

    file.write(f'Удаление значений из request_25 начато в: {time.strftime("%X")}.\n')
    client.execute("alter table request_25 delete where my_date between '2022-04-21' and '2022-04-27';")
    file.write(f'Удаление значений из request_25 закончено в: {time.strftime("%X")}.\n')

    file.write(f'Конец работы: {time.strftime("%X")}.\n')
    file.write(f'Затрачено времени: {round(time.time() - start_time, 3)} сек.\n')

    file.write('-\n')

# select count(uniqueid) from otchet_25 where my_date = toDate('2022-03-29'); - 1168839
# select count(uniqueid) from request_25 where my_date = toDate('2022-03-29'); - 681
