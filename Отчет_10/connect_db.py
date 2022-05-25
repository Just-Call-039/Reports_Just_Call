# Функция для извлечения хоста, логина, пароля.
# Необходимо передать файл с соответствующими значениями.


def connect_db():
    with open(r'C:\Users\Supervisor031\Отчеты\Maria_db.csv') as file:
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
