from mrjob.job import MRJob
import json
import re
import os

class MRWordCount(MRJob):
    # Pre-process of the stopwords
    # Must be included in mapper_init so it can be accesible through the mapper method
    def mapper_init(self):
        base_dir = os.path.dirname(__file__)
        project_root = os.path.dirname(base_dir)      
        stopwords_path = os.path.join(project_root, "stopwords.txt")

        with open(stopwords_path, encoding="utf-8") as f:
            # Unique stopwords without whitespaces
            self.stopwords = set(w.strip() for w in f)

    # _: key, line: value for each line (review) of the document
    def mapper(self, _, line):
        try:
            data = json.loads(line)
            text = data.get("reviewText", "")
            category = data.get("category", "")
        except:
            return
        
        # Divide all the words and lowers them
        tokens = re.split(r"[ \t\d\(\)\[\]\{\}\.\!\?,;:\+=\-_\"'`~#@&\*%€\$§\\/<>^]+",
                          text.lower())

        # Uset set to avoid duplicates
        seen = set()
        for token in tokens:
            # Avoid 1-character words and stopwords
            if len(token) > 1 and token not in self.stopwords:
                seen.add(token)

        # Returns <word>, <category>, 1
        for token in seen:
            yield (token, category), 1

        # Count the number of docs
        yield ("DOC", category), 1

    # To avoid duplicates
    def combiner(self, key, values):
        yield key, sum(values)

    # Returns <word>, <category>, sum across all lines of the document
    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == "__main__":
    MRWordCount.run()