import re
import sys
import json
import heapq

from mrjob.job import MRJob
from mrjob.step import MRStep


# This is not really clean but I tried to move helper functions and connstats to other files but it seems it does not work
# TODO: check this as I used AI to generate this regex pattern
DELIMITER_PATTERN: str = r"""[\s\d()\[\]{}.!?,;:+=\-_"'`~#@&*%€$§\\/]+"""
COMPILED_DELIMITER_PATTERN = re.compile(DELIMITER_PATTERN)
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
    compiled_pattern=COMPILED_DELIMITER_PATTERN,
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
    tokens = compiled_pattern.split(lower_text)
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
        self.add_passthru_arg(
            '--stats_file_path',
            type=str,
            default='stats.json',
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

        for term in unique_terms:
            yield term, (category, 1)

    def pp_combiner(self, term, values):
        category_counts = {}
        for category, count in values:
            category_counts[category] = category_counts.get(category, 0) + count
        for category, count in category_counts.items():
            yield term, (category, count)

    def pp_reducer_init(self):
        with open(self.options.stats_file_path, "r") as f:
            self.stats = json.load(f)
        self.N = self.stats.get("N", 0)

    def pp_reducer(self, term, values):
        """Calculate Chi-Square for the term across all categories it appears in"""
        A_dict = {}
        for category, count in values:
            A_dict[category] = A_dict.get(category, 0) + count
            
        Nt = sum(A_dict.values())
        N = self.N
        
        # Precompute the term-specific part of the denominator outside the loop
        term_denom_base = Nt * (N - Nt)
        
        # If the term appears in EVERY review or NO reviews, variance is 0
        if term_denom_base == 0:
            return
            
        for category, A in A_dict.items():
            Nc = self.stats.get(f"Nc_{category}", 0)
            
            # The fully simplified math operations!
            numerator = (A * N - Nt * Nc) ** 2 * N
            denominator = term_denom_base * Nc * (N - Nc)
            
            if denominator != 0:
                chi_square_value = numerator / denominator
                yield category, (term, chi_square_value)


    ######################
    # Ordering the terms #
    ######################

    def ott_reducer(self, category, term_chi_pairs):
        # Finds the top 75 efficiently without sorting the whole dataset
        top_terms = heapq.nsmallest(
            TOP_K_TERMS, 
            term_chi_pairs, 
            key=lambda x: (-x[1], x[0])  # Sort by chi-sq desc, then term asc
        )
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
                combiner=self.pp_combiner,
                reducer_init=self.pp_reducer_init,
                reducer=self.pp_reducer,
            ),
            MRStep(
                reducer=self.ott_reducer,
            ),
        ]

if __name__ == '__main__':
    ChiSquareJob.run()
