from mrjob.job import MRJob
from itertools import combinations


class MRRecommender(MRJob):

    def mapper(self, key, line):
        purchases = set(line.split(','))
        if len(purchases) > 1:
            for p1, p2 in combinations(purchases, 2):
                yield (p1, p2), 1

    def reducer(self, purchase_pair, occurrences):
        p1, p2 = purchase_pair
        yield p1, (sum(occurrences), p2)

    def reducer_find_best_recos(self, p1, p2_occurences):
        top_products = sorted(p2_occurences, reverse=True)[:5]
        top_products = [p2 for occurences, p2 in top_products]
        yield p1, top_products

    def steps(self):
        return [
            self.mr(mapper=self.mapper,
                    reducer=self.reducer),
            self.mr(reducer=self.reducer_find_best_recos)
        ]


if __name__ == '__main__':
    MRRecommender.run()
