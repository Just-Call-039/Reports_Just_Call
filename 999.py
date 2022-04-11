a = sum(i for i in range(1, 1_000_000_003) if (i % 3 == 0 and (i % 10 != 4 and i % 10 != 7)))
print(a)
