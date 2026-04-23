from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawValueProtocol
import json
import re
from collections import defaultdict


class MRChiSquareAll(MRJob):

    OUTPUT_PROTOCOL = RawValueProtocol

    def configure_args(self):
        super().configure_args()
        self.add_file_arg('--stopwords')

    # =========================
    # STEP 1: term counts + DOC
    # =========================
    def mapper_init_step1(self):
        with open(self.options.stopwords) as f:
            self.stopwords = set(w.strip() for w in f)

    def mapper_step1(self, _, line):
        try:
            data = json.loads(line)
            text = data.get("reviewText", "")
            category = data.get("category", "")
        except:
            return

        tokens = re.split(
            r"[ \t\d\(\)\[\]\{\}\.\!\?,;:\+=\-_\"'`~#@&\*%€\$§\\/<>^]+",
            text.lower()
        )

        seen = set()
        for token in tokens:
            if len(token) > 1 and token not in self.stopwords:
                seen.add(token)

        # términos
        for token in seen:
            yield (token, category), 1

        # documentos
        yield ("DOC", category), 1

    def combiner_step1(self, key, values):
        yield key, sum(values)

    def reducer_step1(self, key, values):
        yield key, sum(values)

    # =========================
    # STEP 2: aggregations
    # =========================
    def mapper_step2(self, key, count):

        # Caso DOC
        if key[0] == "DOC":
            _, category = key
            yield ("DOC_CAT", category), count
            yield ("DOC_TOTAL", "ALL"), count

        else:
            term, category = key
            yield ("TERM", term), count
            yield ("TERM_CAT", term, category), count

    def reducer_step2(self, key, values):
        yield key, sum(values)

    # =========================
    # STEP 3: chi-square
    # =========================
    def mapper_step3(self, key, count):
        yield "ALL", (key, count)

    def reducer_step3(self, _, values):
        N = 0
        N_t = defaultdict(int)
        N_c = defaultdict(int)
        N_tc = {}

        for key, count in values:
            if key[0] == "DOC_TOTAL":
                N = count
            elif key[0] == "DOC_CAT":
                N_c[key[1]] = count
            elif key[0] == "TERM":
                N_t[key[1]] = count
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

        # output per category
        for category in sorted(results.keys()):
            top_terms = sorted(results[category], key=lambda x: -x[1])[:75]

            output = category
            for term, score in top_terms:
                output += f" {term}:{score:.4f}"

            yield None, output

        # global dictionanry
        all_terms = sorted(N_t.keys())
        yield None, "dictionary: " + " ".join(all_terms)

    # =========================
    # PIPELINE
    # =========================
    def steps(self):
        return [
            MRStep(
                mapper_init=self.mapper_init_step1,
                mapper=self.mapper_step1,
                combiner=self.combiner_step1,
                reducer=self.reducer_step1
            ),
            MRStep(
                mapper=self.mapper_step2,
                reducer=self.reducer_step2
            ),
            MRStep(
                mapper=self.mapper_step3,
                reducer=self.reducer_step3
            )
        ]


if __name__ == "__main__":
    MRChiSquareAll.run()