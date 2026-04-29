# Data-Intensive-Computing

This project computes, for each review category, the top 75 terms with the highest chi-square score and also produces a merged dictionary of all top-ranked terms across categories.

The pipeline is implemented with [MRJob](https://mrjob.readthedocs.io/) and is split into two MapReduce jobs:

1. A global statistics job that counts the total number of reviews and the number of reviews per category.
2. A chi-square job that tokenizes review text, removes stopwords, scores terms per category, and keeps the strongest 75 terms for each category.

## Repository Contents

- `main.py`: entry point that runs both jobs and writes the final output file.
- `global_stats_job.py`: first MapReduce job that computes global counts.
- `chi_square_job.py`: second MapReduce job that computes chi-square scores and top terms.
- `bash_script.sh`: convenience wrapper for local or Hadoop execution.
- `reviews_devset.json`: smaller development dataset for local testing.
- `stopwords.txt`: stopword list used during tokenization.
- `stats.json`: generated intermediate file containing global counts.
- `output.txt`: generated final output file.

## What the Program Does

For each review, the program expects a JSON object containing at least:

- `category`: review category label
- `reviewText`: raw review text

The workflow is:

1. Read the review dataset.
2. Count the total number of reviews, `N`.
3. Count the number of reviews in each category, `Nc`.
4. Tokenize each review text into unique terms.
5. Remove stopwords and short tokens.
6. Compute chi-square scores for each term-category pair.
7. Keep the top 75 terms per category.
8. Write the per-category results and a merged alphabetic dictionary of all selected terms.

## Output Format

The final output file contains one line per category, followed by a final line containing the merged dictionary.

Example structure:

```text
Apps_for_Android games:3081.1493 play:2158.3694 ...
Automotive oem:1068.8584 honda:1035.2234 ...
Baby diaper:2429.7403 crib:2411.4696 ...
...
acdelco acne acoustic ... zoom
```

Each category line has the form:

```text
<category> <term1>:<score1> <term2>:<score2> ...
```

The final line is a space-separated, alphabetically sorted list of all terms selected in the category outputs.

## Requirements

The project targets Python 3.12 and depends on:

- `mrjob==0.7.4`
- `packaging==26.1`
- `setuptools>=68.0.0`

The recommended local workflow uses `uv`, which is referenced in `bash_script.sh`.

## How To Run

### Local run

Use the development dataset shipped with the repository:

```bash
sh src/bash_script.sh
```

The script sets the local execution path, runs `main.py`, and writes the result to `output.txt`.

If you want to call the program manually, you can run:

```bash
python -m main \
	--is_local true \
	--data_file_path reviews_devset.json \
	--stopwords_file_path stopwords.txt \
	--output_file_path output.txt
```

### Hadoop / cluster run

`bash_script.sh` also shows the cluster configuration used in this project. In cluster mode, the input is expected to come from Hadoop-compatible storage and the jobs use the configured Hadoop streaming jar and Python interpreter path.

Important values in the script:

- `DEV_DATA_FILE_PATH`: development dataset in HDFS
- `FULL_DATA_FILE_PATH`: full dataset in HDFS
- `STOPWORDS`: stopword file path
- `N_MAPPERS` and `N_REDUCERS`: job parallelism settings in `main.py`

The cluster branch of `main.py` passes `stopwords.txt` and the generated `stats.json` to the remote job so both steps can run with the same auxiliary data.

## How The Code Is Organized

### `main.py`

`main.py` coordinates the full pipeline:

1. It parses command-line arguments.
2. It runs `GlobalStatsJob` to create `stats.json`.
3. It runs `ChiSquareJob` using that statistics file.
4. It collects and sorts the results.
5. It writes the final per-category output and merged dictionary.

### `global_stats_job.py`

This job scans each line of the input dataset and extracts the `category` field using a regular expression. For every review, it emits:

- `N = 1` for the global review count
- `Nc_<category> = 1` for the category-specific count

The reducer sums those values to produce the final statistics file.

### `chi_square_job.py`

This job performs the term scoring work.

Key steps:

1. Load stopwords during mapper initialization.
2. Parse each review JSON record.
3. Extract the review category and review text.
4. Normalize text to lowercase and split on a delimiter regex.
5. Deduplicate tokens per review so each term is counted at most once per review.
6. Aggregate term counts per category.
7. Load the previously generated `stats.json` in the reducer.
8. Compute chi-square scores for each term-category pair.
9. Keep the top 75 terms per category using a heap-based selection.

## Tokenization And Filtering

Tokenization is controlled by a delimiter regex in `chi_square_job.py`.

Filtering rules:

- Convert text to lowercase.
- Split on whitespace, punctuation, digits, and other delimiter characters.
- Remove tokens shorter than the minimum token length.
- Remove stopwords loaded from `stopwords.txt`.
- Deduplicate terms within a single review.

This design means the chi-square score is based on review presence, not raw term frequency inside a review.

## Chi-Square Scoring

For each term and category, the reducer computes a chi-square score using the counts:

- `A`: number of reviews in the category containing the term
- `Nt`: total number of reviews containing the term
- `Nc`: number of reviews in the category
- `N`: total number of reviews overall

The formula implemented in the code is:

```text
X^2 = N * (A * N - Nt * Nc)^2 / (Nt * (N - Nt) * Nc * (N - Nc))
```

Terms that appear in every review or in no reviews are skipped because they do not provide a meaningful variance signal.

## Configuration Notes

- `--is_local`: set to `true` for local execution, `false` for Hadoop execution.
- `--data_file_path`: input dataset path.
- `--stopwords_file_path`: stopword file location.
- `--output_file_path`: destination for the final output.

The second job also accepts:

- `--stats_file_path`: path to the generated global statistics JSON file.

## Generated Files

Running the pipeline creates or updates:

- `stats.json`: intermediate counts used by the chi-square job.
- `output.txt`: final ranked term output.

These files can be safely regenerated by rerunning the pipeline.

## Troubleshooting

- If the input file is malformed JSON, the chi-square job will fail while parsing a line.
- If `stats.json` is missing, the second job cannot compute scores.
- If the stopword file path is wrong, mapper initialization will fail before processing data.
- If you run in cluster mode, verify that the Hadoop streaming jar and Python path in `main.py` match your environment.

## Notes

The repository currently uses a compact two-job design to keep the pipeline easy to follow:

- first job: compute category statistics
- second job: score and rank terms

This makes the workflow easy to reproduce locally while still matching the shape of a larger distributed data-processing pipeline.
