from mrjob.job import MRJob
from itertools import permutations


class MRRecommender(MRJob):

    def mapper(self, key, line):
        account_id, purchases = line.split()
        purchases = set(purchases.split(','))
        for p1, p2 in permutations(purchases, 2):
            yield (account_id, p1, p2), 1

    def reducer(self, purchase_pair, occurrences):
        account_id, p1, p2 = purchase_pair
        yield (account_id, p1), (sum(occurrences), p2)

    def reducer_find_best_recos(self, key, p2_occurences):
        top_products = sorted(p2_occurences, reverse=True)[:5]
        top_products = [p2 for occurences, p2 in top_products]
        yield key, top_products

    def steps(self):
        return [self.mr(mapper=self.mapper, reducer=self.reducer),
                self.mr(reducer=self.reducer_find_best_recos)]


if __name__ == '__main__':
    MRRecommender.run()
