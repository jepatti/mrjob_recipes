import random

for i in range(1000):
    purchased = random.randint(1,5)
    print ','.join(str(random.randint(1,20)) for _ in range(purchased))
