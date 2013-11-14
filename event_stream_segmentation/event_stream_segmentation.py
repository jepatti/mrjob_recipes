from mrjob.job import MRJob


class MRWordCounter(MRJob):

    def mapper(self, key, line):
        user_id, purchased, time_on_site = line.split()
        yield user_id, timestamp

    def reducer(self, word, occurrences):
        yield word, sorted(occurrences)


if __name__ == '__main__':
    MRWordCounter.run()
