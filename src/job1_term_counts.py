from mrjob.job import MRJob
import json
import re
import os

class MRWordCount(MRJob):
    def mapper_init(self):
        base_dir = os.path.dirname(__file__)
        base_dir = os.path.dirname(__file__)
        project_root = os.path.dirname(base_dir)      
        stopwords_path = os.path.join(project_root, "stopwords.txt")

        with open(stopwords_path, encoding="utf-8") as f:
            self.stopwords = set(w.strip() for w in f)

    def mapper(self, _, line):
        try:
            data = json.loads(line)
            text = data.get("reviewText", "")
            category = data.get("category", "")
        except:
            return
        
        tokens = re.split(r"[ \t\d\(\)\[\]\{\}\.\!\?,;:\+=\-_\"'`~#@&\*%€\$§\\/]+",
                          text.lower())

        seen = set()
        for token in tokens:
            if len(token) > 1 and token not in self.stopwords:
                seen.add(token)

        for token in seen:
            yield (token, category), 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == "__main__":
    MRWordCount.run()