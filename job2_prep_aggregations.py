from mrjob.job import MRJob
import json

class MRChiSquarePrep(MRJob):
    def mapper(self, _, line):
        key_str, count_str = line.strip().split("\t")
        key = json.loads(key_str)
        count = int(count_str)
        term, category = key

        yield ("TERM", term), count

        yield ("CATEGORY", category), count

        yield ("TERM_CAT", term, category), count

        yield ("TOTAL", "ALL"), count

    def reucer(self, key, values):
        yield key, sum(values)

if __name__=="__main__":
    MRChiSquarePrep.run()