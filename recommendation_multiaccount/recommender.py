from mrjob.job import MRJob
from itertools import permutations


class MRRecommender(MRJob):

    def mapper(self, key, line):
        account_id, purchases = line.split()
        purchases = set(purchases.split(','))
        for p1, p2 in permutations(purchases, 2):
            yield (account_id, p1, p2), 1

    def reducer(self, word, occurrences):
        account_id, p1, p2 = word
        yield (account_id, p1), (p2, sum(occurrences))

if __name__ == '__main__':
    MRRecommender.run()
