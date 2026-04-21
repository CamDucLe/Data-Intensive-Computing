import json
from mrjob.job import MRJob
from collections import defaultdict
from mrjob.protocol import RawValueProtocol

class MRChiSquareFinal(MRJob):
    # Allows to save the results in the desired format
    OUTPUT_PROTOCOL = RawValueProtocol

    # Works with the output of the Job 2
    # ["TERM", <word>] sum of count
    # ["CATEGORY", <category_name>] sum of count
    # ["TERM_CAT", <word>, <category_name>] sum of count
    # ["TOTAL", "ALL"] count of all words
    def mapper(self, _, line):
        key_str, count_str = line.strip().split('\t')
        key = json.loads(key_str)
        count = int(count_str)
        yield "ALL", (key, count)

    # Returns all to the reducer in the form:
    # ALL, ["TERM", <word>] sum of count
    # ALL, ["CATEGORY", <category_name>] sum of count
    # ALL, ["TERM_CAT", <word>, <category_name>] sum of count
    # ALL, ["TOTAL", "ALL"] count of all words
    def reducer(self, _, values):
        N = 0
        N_t = defaultdict(int)
        N_c = defaultdict(int)
        N_tc = {}

        for key, count in values:
            if key[0] == "TOTAL":
                N = count
            elif key[0] == "TERM":
                N_t[key[1]] = count
            elif key[0] == "CATEGORY":
                N_c[key[1]] = count
            elif key[0] == "TERM_CAT":
                term, category = key[1], key[2]
                N_tc[(term, category)] = count

        results = defaultdict(list)

        for (term, category), A in N_tc.items():
            Nt = N_t[term]
            Nc = N_c[category]

            B = Nt - A
            C = Nc - A
            D = N - Nt - Nc + A

            if (A+B)*(C+D)*(A+C)*(B+D) == 0:
                continue

            chi2 = (N * (A*D - B*C)**2) / ((A+B)*(C+D)*(A+C)*(B+D))
            results[category].append((term, chi2))

        for category in sorted(results.keys()):
            top_terms = sorted(results[category], key=lambda x: -x[1])[:75]

            output = category
            for term, score in top_terms:
                output += f" {term}:{score:.4f}"

            yield None, output

        all_terms = sorted(N_t.keys())
        dictionary_line = "dictionary: " + " ".join(all_terms)

        yield None, dictionary_line


if __name__ == "__main__":
    MRChiSquareFinal.run()