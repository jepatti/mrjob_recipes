from mrjob.job import MRJob
from itertools import combinations


class MRRecommender(MRJob):

    def mapper(self, key, line):
        account_id, purchases = set(line.split(','))
        if len(purchases) > 1:
            for p1, p2 in combinations(purchases, 2):
                yield (account_id, p1, p2), 1

    def reducer(self, word, occurrences):
        account_id, p1, p2 = word
        yield (account_id, p1), (p2, sum(occurrences))

if __name__ == '__main__':
    MRRecommender.run()
