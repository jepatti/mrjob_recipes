import random

# timestamp user_id
start_time = 1384387427

for i in range(10000):
    print start_time + i, (i % 1000) + random.randint(0, 20)