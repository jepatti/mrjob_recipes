import random

for user_id in range(1000):
    campaign_ids = ','.join(str(random.randint(0,5)) for _ in range(random.randint(1,3)))
    purchased = random.randint(0,1)
    session_start_time = random.randint(100000, 200000)
    session_end_time = session_start_time + random.randint(0, 500)
    print campaign_ids, user_id, purchased, session_start_time, session_end_time