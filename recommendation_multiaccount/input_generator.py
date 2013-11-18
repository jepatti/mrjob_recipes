import random

for i in range(1000):
    account_id = random.randint(0,9)
    purchased = random.randint(1,5)
    print account_id, ','.join(str(random.randint(1,20)) for _ in range(purchased))
