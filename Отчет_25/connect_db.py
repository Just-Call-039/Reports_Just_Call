# Функция для извлечения хоста, логина, пароля.
# Необходимо передать файл с соответствующим наименованием.
# Maria_db, 72, Combat, Click.


def connect_db(file):
    dest = None
    if file == 'Maria_db':
        dest = r'C:\Users\Supervisor031\Отчеты\Maria_db.csv'
    elif file == '72':
        dest = r'C:\Users\Supervisor031\Отчеты\Second_cloud_72.csv'
    elif file == 'Combat':
        dest = r'C:\Users\Supervisor031\Отчеты\Combat_server.csv'
    elif file == 'Click':
        dest = r'C:\Users\Supervisor031\Отчеты\ClickHouse.csv'
    else:
        print('Неизвестный сервер.')

    if dest:
        with open(dest) as file:
            for now in file:
                now = now.strip().split('=')
                first, second = now[0].strip(), now[1].strip()
                if first == 'host':
                    host = second
                elif first == 'user':
                    user = second
                elif first == 'password':
                    password = second
        return host, user, password
