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

        if key[0] == "DOC":
            _, category = key

            # Reviews per category
            yield ("DOC_CAT", category), count

            # Total reviews
            yield ("DOC_TOTAL", "ALL"), count

        else:
            term, category = key

            # Reviews per term
            yield ("TERM", term), count

            # Reviews per Term + Category (A in chi-square)
            yield ("TERM_CAT", term, category), count

if __name__=="__main__":
    MRChiSquarePrep.run()