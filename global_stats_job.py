import re

from mrjob.job import MRJob


class GlobalStatsJob(MRJob):
    def mapper(self, _, line):
        # Extremely fast regex extraction bypassing full JSON parsing
        match = re.search(r'"category":\s*"([^"]+)"', line)
        if match:
            category = match.group(1)
            # "N" tracks the total count of reviews
            yield "N", 1
            # "Nc_..." tracks counts reviews per specific categories
            yield f"Nc_{category}", 1

    def combiner(self, key, values):
        # Local aggregation to minimize data transferred over the network
        yield key, sum(values)

    def reducer(self, key, values):
        # Final aggregation to produce the total counts per key
        yield key, sum(values)


if __name__ == "__main__":
    GlobalStatsJob.run()
