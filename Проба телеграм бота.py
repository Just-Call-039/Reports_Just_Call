import telegram_send
import time

a = sum([i / 2 for i in range(50, 1000)])
b = sum([i for i in range(50, 10000) if i % 2 == 0])

telegram_send.send(messages=[
    f'{a}, {b}\n'
    f'Производится подключение к БД ClickHouse.\n'
    f'Дата: {time.strftime("%d-%m-%Y")}. Время: {time.strftime("%X")}.'])
now_time = time.time()

try:
    # c = 1 / 0
    c = sum([i for i in range(100_000_000) if i % 2 == 0])
    telegram_send.send(messages=[f'{c}'])
except:
    telegram_send.send(messages=['Не удалось.'])

telegram_send.send(messages=[f'Подключение выполнено. Ушло времени: {round(time.time() - now_time, 3)} сек.\n'])
