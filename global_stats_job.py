import re

from mrjob.job import MRJob


class GlobalStatsJob(MRJob):
    """
    MapReduce job to compute global statistics needed for chi-square calculation.

    Produces a JSON file containing:
        - N:          total number of reviews across all categories
        - Nc_<cat>:   number of reviews per category

    These statistics are computed by bypassing full JSON parsing only
    the 'category' field is extracted via regex, since reviewText
    is not needed here.
    """

    def mapper(self, _, line):
        """
        Extract the category from a raw review line and emit review counts.

        Bypasses json.loads() entirely and instead uses a targeted regex to extract
        only the 'category' field, which is the only field needed by this job.

        Input:
            - str line (raw JSON line representing one review)

        Output:
            ("N",          1) - contributes to the total review count
            ("Nc_<cat>",   1) - contributes to the per-category review count
        """
        match = re.search(r'"category":\s*"([^"]+)"', line)
        if match:
            category = match.group(1)
            yield "N", 1
            yield f"Nc_{category}", 1

    def combiner(self, key, values):
        """
        Locally aggregate counts before the shuffle to reduce network traffic.

        Runs on the same node as the mapper, merging counts for the same key
        before they are sent across the network to the reducer.

        Input:
            key:    "N" or "Nc_<category>"
            values: integer (partial counts from local mappers)

        Output:
            (key, int) - same key but locally summed count
        """
        yield key, sum(values)

    def reducer(self, key, values):
        """
        Globally aggregate counts to produce final totals stats.

        Receives all (possibly combiner-aggregated) counts for a given key
        from across all mappers and produces the final sum.

        Input:
            key:    "N" or "Nc_<category>"
            values: integer (partial counts from all combiners)

        Output:
            (key, int) - final total count for that key
        """
        yield key, sum(values)


if __name__ == "__main__":
    GlobalStatsJob.run()
