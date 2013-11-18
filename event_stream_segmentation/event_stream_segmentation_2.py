from mrjob.job import MRJob

MAX_SESSION_INACTIVITY = 60 * 5

class MRWordCounter(MRJob):

    def mapper(self, key, line):
        timestamp, user_id = line.split()
        yield user_id, int(timestamp)

    def reducer(self, uid, timestamps):
        timestamps = sorted(timestamps)
        start_index = 0
        for index, timestamp in enumerate(timestamps):
            if index > 0:
                if timestamp - timestamps[index-1] > MAX_SESSION_INACTIVITY:
                    yield uid, timestamps[start_index:index]
                    start_index = index
        yield uid, timestamps[start_index:]

if __name__ == '__main__':
    MRWordCounter.run()
