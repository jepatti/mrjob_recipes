from mrjob.job import MRJob
from itertools import combinations


class MRStatistics(MRJob):

    def mapper(self, key, line):
        # parse input line
        account_id, experiment_ids, user_id, purchased, session_start_time, session_end_time = line.split()
        experiment_ids = map(int, experiment_ids.split(','))
        purchased = int(purchased)
        session_duration = int(session_end_time) - int(session_start_time)

        # yield out a stats value for each campaign this user viewed
        for experiment_id in experiment_ids:
            # y^0, y^1, y^2 - session count, purchases, purchases
            yield (account_id, experiment_id, 'conversion rate'), (1, purchased, purchased) # purchased ^ 2 = purchased

            # y^0, y^1, y^2 - session count, sum session times, sum of squares of session times
            yield (account_id, experiment_id, 'average session length'), (1, session_duration, session_duration ** 2)


    def reducer(self, metric, metric_values):
        # sum up all the statistics for this (account_id, experiment_id, metric type)
        yield metric, reduce(lambda x, y: map(sum, zip(x, y)), metric_values)

if __name__ == '__main__':
    MRStatistics.run()
