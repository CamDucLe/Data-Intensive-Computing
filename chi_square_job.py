import re
import sys
import json

from mrjob.job import MRJob
from mrjob.step import MRStep


# This is not really clean but I tried to move helper functions and connstats to other files but it seems it does not work
# TODO: check this as I used AI to generate this regex pattern
DELIMITER_PATTERN: str = r"""[\s\d()\[\]{}.!?,;:+=\-_"'`~#@&*%€$§\\/]+"""
MIN_TOKEN_LENGTH: int = 2
TOP_K_TERMS: int = 75


def load_stopwords(
    file_path: str,
) -> set[str]:
    """Load stopwords from a text file and return a set of stopwords"""
    with open(file_path) as txt_file:
        stopwords = {line.strip() for line in txt_file if line.strip()}

    return stopwords


def preprocess_text(
    text: str,
    stopwords: set[str],
    delimiter_pattern: str = DELIMITER_PATTERN,
    min_token_length: int = MIN_TOKEN_LENGTH,
) -> set[str]:
    """
    Preprocess a review text by applying the following steps:
        1. Case-fold to lowercase
        2. Tokenize using the predefined delimiter regex pattern
        3. Remove stopwords and tokens shorter than the minimum token length
        4. Deduplicate tokens to get a set of unique terms (for chi-square calculation)

    Args:
        text: The raw review text to preprocess
        stopwords: A set of stopwords to remove from the text
        delimiter_pattern: A regex pattern to use for tokenisation
        min_token_length: Minimum length of tokens to keep

    Returns:
        A set of unique, preprocessed terms extracted from the review text
    """
    lower_text = text.lower()
    tokens = re.split(delimiter_pattern, lower_text)
    unique_terms = {
        token
        for token in tokens
        if len(token) >= min_token_length and token not in stopwords
    }

    return unique_terms


class ChiSquareJob(MRJob):
    def configure_args(self):
        """Register custom command-line arguments for external data files."""
        super().configure_args()

        self.add_passthru_arg(
            '--stopwords_file_path',
            type=str,
            default='stopwords.txt',
        )

    #########################
    # Reviews preprocessing #
    #########################
    def pp_mapper_init(self):
        self.stopwords = load_stopwords(self.options.stopwords_file_path)

    def pp_mapper(self, _, line):
        """Preprocess each review text and emit (term, category) counts for each unique term in the review"""
        # Load a review (a line of the input file) as a json object
        review = json.loads(line)

        category = review["category"]
        text = review["reviewText"]

        unique_terms = preprocess_text(text, self.stopwords)

        # Yield total review count for later use in chi-square calculation
        yield ("<<N>>", None), 1

        # Yield category-specific review count for later use in chi-square calculation
        yield ("<<Nc>>", category), 1

        for term in unique_terms:
            # Yield total term exists in category for later use in chi-square calculation
            yield (term, category), 1  # A

            # Yeild total reviews containing term for later use in chi-square calculation
            yield ("<<Nt>>", term), 1

    def pp_reducer(self, key, values):
        """Pass through global stats and category counts to the next step"""
        # Do this because if simply use "yield key, sum(values)" <<N>>, <<Nc>>, <<Nt>> will be sent to different reducers
        # But we need to ensure they are processed by the same reducer to compute global statistics for computing chi-square correctly in the next step
        # Use this trick to ensure all global stats and category counts are sent to the same reducer by using a common key ("<<GLOBAL_STATS>>") for them
        yield "<<GLOBAL_STATS>>", (key, sum(values))


    ##########################
    # Chi-square calculation #
    ##########################
    def ccs_reducer_init(self):
        self.N = 0
        self.Nc = {}
        self.Nt = {}
        self.A = {}

    def ccs_reducer(self, key, values):
        for original_key, value in values:
            k0, k1 = original_key

            if k0 == "<<N>>":
                self.N += value
            elif k0 == "<<Nc>>":
                self.Nc[k1] = self.Nc.get(k1, 0) + value
            elif k0 == "<<Nt>>":
                self.Nt[k1] = self.Nt.get(k1, 0) + value
            else:
                # original_key is (term, category)
                term, category = original_key
                self.A[(term, category)] = self.A.get((term, category), 0) + value

    def ccs_reducer_final(self):
        """
        Compute chi-square statistic for a term against every categories.

        Chi-square formula (2x2 contingency table):
            X^2(t, c) = N x (A*D - B*C)² / ((A+B)(C+D)(A+C)(B+D))

        Where:
            A = reviews in category c that contain term t
            C = reviews in category c that do NOT contain t = total_reviews_in_category - A
            B = reviews NOT in category c that contain t = total_reviews_containing_term - A
            D = reviews NOT in category c and NOT containing t = total_reviews - (A + B + C)
        """
        # After processing all keys, we have the global stats and category counts
        # which are needed to compute chi-square for every (term, category) pair

        total_reviews = self.N
        for (term, category), A in self.A.items():
            total_reviews_in_category = self.Nc.get(category, 0)
            total_reviews_containing_term = self.Nt.get(term, 0)

            # Derive B, C, D from A and the global stats
            B = total_reviews_containing_term - A
            C = total_reviews_in_category - A
            D = total_reviews - (A + B + C)

            # Compute chi-square statistic for this (term, category) pair
            numerator = (A * D - B * C) ** 2 * (A + B + C + D)
            denominator = (A + B) * (C + D) * (A + C) * (B + D)
            if denominator == 0:
                continue
            else:
                chi_square_value = numerator / denominator
                yield category, (term, chi_square_value)


    ######################
    # Ordering the terms #
    ######################
    def ott_reducer(self, category, term_chi_pairs):
        """
        One line for each product category (categories in alphabetic order),
        that contains the top 75 most discriminative terms for the category
        according to the chi-square test in descending order
        """
        # Convert term_chi_pairs from generator to list to sort and select top K terms
        term_chi_pairs = list(term_chi_pairs)

        # Sort and select top K terms with highest chi-square values for this category
        term_chi_pairs.sort(key=lambda x: (-x[1], x[0]))
        top_terms = term_chi_pairs[:TOP_K_TERMS]

        # Format the output as term_1st:chi^2_value term_2nd:chi^2_value ... term_75th:chi^2_value
        formatted_top_terms = ' '.join(f"{term}:{score:.4f}" for term, score in top_terms)

        yield category, formatted_top_terms


    #######################
    # Pipeline definition #
    #######################
    def steps(self):
        """
        Define the two steps of the MapReduce job.

        Reference: https://mrjob.readthedocs.io/en/latest/guides/writing-mrjobs.html#defining-steps
        """
        return [
            MRStep(
                mapper_init=self.pp_mapper_init,
                mapper=self.pp_mapper,
                reducer=self.pp_reducer,
            ),
            MRStep(
                reducer_init=self.ccs_reducer_init,
                reducer=self.ccs_reducer,
                reducer_final=self.ccs_reducer_final,
            ),
            MRStep(
                reducer=self.ott_reducer,
            ),
        ]

if __name__ == '__main__':
    ChiSquareJob.run()
