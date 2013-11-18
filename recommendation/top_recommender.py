from mrjob.job import MRJob
from itertools import permutations


class MRRecommender(MRJob):

    def mapper(self, key, line):
        purchases = set(line.split(','))
        for p1, p2 in permutations(purchases, 2):
            yield (p1, p2), 1

    def reducer(self, pair, occurrences):
        p1, p2 = pair
        yield p1, (sum(occurrences), p2)

    def reducer_find_best_recos(self, p1, p2_occurrences):
        top_products = sorted(p2_occurrences, reverse=True)[:5]
        top_products = [p2 for occurrences, p2 in top_products]
        yield p1, top_products

    def steps(self):
        return [self.mr(mapper=self.mapper, reducer=self.reducer),
                self.mr(reducer=self.reducer_find_best_recos)]


if __name__ == '__main__':
    MRRecommender.run()
