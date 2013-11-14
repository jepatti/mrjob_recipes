from mrjob.job import MRJob
from itertools import combinations


class MRRecommender(MRJob):

    def mapper(self, key, line):
        campaign_ids, user_id, purchased, session_start_time, session_end_time = line.split()
        campaign_ids = map(int, campaign_ids.split(','))
        purchased = int(purchased)
        session_duration = int(session_end_time) - int(session_start_time)

        for campaign_id in campaign_ids:
            # statistic for average order value
            # y^0, y^1, y^2 - session count, purchases, purchases
            yield (campaign_id, 'average order value'), (1, purchased, purchased) # purchased ^ 2 = purchased

            # statistic for average session length
            # y^0, y^1, y^2 - session count, sum session times, sum of squares of session times
            yield (campaign_id, 'average session length'), (1, session_duration, session_duration ** 2)


    def reducer(self, metric, metric_values):
        yield metric, reduce(lambda x, y: map(sum, zip(x, y)), metric_values)

if __name__ == '__main__':
    MRRecommender.run()
