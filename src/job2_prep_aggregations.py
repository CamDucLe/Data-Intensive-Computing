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

        # Caso especial: conteo de documentos
        if key[0] == "DOC":
            _, category = key

            # número de documentos por categoría
            yield ("DOC_CAT", category), count

            # total de documentos
            yield ("DOC_TOTAL", "ALL"), count

        else:
            term, category = key

            # total por término
            yield ("TERM", term), count

            # total por categoría (en términos, opcional)
            yield ("CATEGORY", category), count

            # término + categoría (A en chi-square)
            yield ("TERM_CAT", term, category), count

if __name__=="__main__":
    MRChiSquarePrep.run()