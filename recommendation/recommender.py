from mrjob.job import MRJob
from itertools import permutations


class MRRecommender(MRJob):

    def mapper(self, key, line):
        purchases = set(line.split(','))
        for p1, p2 in permutations(purchases, 2):
            yield (p1, p2), 1

    def reducer(self, pair, occurrences):
        p1, p2 = pair
        yield p1, (p2, sum(occurrences))

if __name__ == '__main__':
    MRRecommender.run()
