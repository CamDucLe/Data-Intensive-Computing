import re
from mrjob.job import MRJob

class GlobalStatsJob(MRJob):
    def mapper(self, _, line):
        # Extremely fast regex extraction bypassing full JSON parsing
        match = re.search(r'"category":\s*"([^"]+)"', line)
        if match:
            category = match.group(1)
            yield "N", 1
            yield f"Nc_{category}", 1

    def combiner(self, key, values):
        yield key, sum(values)

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    GlobalStatsJob.run()
