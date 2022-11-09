# Функция для извлечения хоста, логина, пароля.
# Необходимо передать файл с соответствующим наименованием.
# Maria_db, 72, Combat, Click, Server_MySQL, DBS, cloud_117.


def connect_db(file):
    dest = None
    if file == 'Maria_db':
        dest = r'D:\Отчеты\not_share\Maria_db.csv'
    elif file == 'cloud_117':
        dest = r'D:\Отчеты\not_share\cloud_my_sql_117.csv'
    elif file == '72':
        dest = r'D:\Отчеты\not_share\Second_cloud_72.csv'
    elif file == 'Combat':
        dest = r'D:\Отчеты\not_share\Combat_server.csv'
    elif file == 'Click':
        dest = r'D:\Отчеты\not_share\ClickHouse.csv'
    elif file == 'Server_MySQL':
        dest = r'D:\Отчеты\not_share\Server_files_MySQL.csv'
    elif file == 'DBS':
        dest = r'D:\Отчеты\not_share\DBS.csv'
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
