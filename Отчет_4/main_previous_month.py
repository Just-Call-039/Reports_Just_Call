import datetime

today = datetime.date.today()
year = today.year
month = today.month - 1

print(f'{year}_{month}')
