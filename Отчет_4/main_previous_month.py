import datetime
import dateutil.relativedelta

today = datetime.date.today()
year = today.year
month = today.month

test_date = '2023-01-01'
test_date = datetime.datetime.strptime(test_date, '%Y-%m-%d')
test_2 = today - dateutil.relativedelta.relativedelta(months=1)

year_2 = test_2.year
month_2 = test_2.month

print(f'{year}_{month}')
print(test_date)
print(test_2)
print(f'{year_2}_{month_2}.csv')
print(today)
