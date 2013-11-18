from mrjob.job import MRJob
from itertools import combinations


class MRStatistics(MRJob):

    def mapper(self, key, line):
        account_id, user_id, purchased, session_start_time, session_end_time = line.split()
        purchased = int(purchased)
        session_duration = int(session_end_time) - int(session_start_time)

        # y^0, y^1, y^2 - session count, purchased, purchased
        yield (account_id, 'conversion rate'), (1, purchased, purchased) # purchased ^ 2 = purchased

        # y^0, y^1, y^2 - session count, sum session times, sum of squares of session times
        yield (account_id, 'average session length'), (1, session_duration, session_duration ** 2)


    def reducer(self, metric, metric_values):
        # for metric, yield [sum(y^0), sum(y^1), sum(y^2)]
        yield metric, reduce(lambda x, y: map(sum, zip(x, y)), metric_values)

if __name__ == '__main__':
    MRStatistics.run()
