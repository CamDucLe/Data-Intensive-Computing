from mrjob.job import MRJob
import json

class MRChiSquarePrep(MRJob):
    # Works with the output of the Job 1
    # _: key
    # line: <word>, <category>, sum across all lines of the document
    def mapper(self, _, line):
        key_str, count_str = line.strip().split("\t")
        key = json.loads(key_str)
        count = int(count_str)
        term, category = key

        yield ("TERM", term), count

        yield ("CATEGORY", category), count

        yield ("TERM_CAT", term, category), count

        yield ("TOTAL", "ALL"), count

    # For each:
    # ["TERM", <word>] count
    # ["CATEGORY", <category_name>] count
    # ["TERM_CAT", <word>, <category_name>] count
    # ["TOTAL", "ALL"] count (the total count)

    # Returns the sum of the values, obtaining:
    # ["TERM", <word>] sum of count
    # ["CATEGORY", <category_name>] sum of count
    # ["TERM_CAT", <word>, <category_name>] sum of count
    # ["TOTAL", "ALL"] count of words
    def reducer(self, key, values):
        yield key, sum(values)

if __name__=="__main__":
    MRChiSquarePrep.run()