from mrjob.job import MRJob
from itertools import combinations


class MRRecommender(MRJob):

    def mapper(self, key, line):
        purchases = set(line.split(','))
        if len(purchases) > 1:
            for p1, p2 in combinations(purchases, 2):
                yield (p1, p2), 1

    def reducer(self, word, occurrences):
        p1, p2 = word
        yield p1, (p2, sum(occurrences))

if __name__ == '__main__':
    MRRecommender.run()
