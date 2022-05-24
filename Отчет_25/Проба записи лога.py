import time

word = input()

with open(r'C:\Users\Supervisor031\Отчеты\my_del_2.txt', 'a') as file:
    start = '--------------start--------------\n'
    file.write(start)
    file.write(f'Дата: {time.strftime("%d-%m-%Y")}. Время: {time.strftime("%X")}.\n')
    file.write(f'{word} \n')
    end = '---------------end---------------\n'
    file.write(end)
    file.write('\n')
