from datetime import date
import dateutil.relativedelta

today = date.today()

year = today.strftime('%Y')
month = today.strftime('%m')
day = today.strftime('%d')

file_name = f'{year}_{month}_{day}.csv'
print(file_name)

today = date.today()
previous_date = today - dateutil.relativedelta.relativedelta(months=1)
year = previous_date.year
month = previous_date.month
file_name = f'{year}_{month}.csv'

print(file_name)
