# Chi-Square Feature Selection with MapReduce

This project implements a **MapReduce pipeline using Python and mrjob** to compute the **Chi-Square (χ²) statistic** for term-category association on a dataset of product reviews.

The goal is to identify the **top 75 most relevant terms per category** and generate a **global dictionary of terms**.

---

## Project Structure
```
├── job1_term_counts.py      # Term-document counts
├── job2_chi_squared.py      # Aggregates global statistics
├── job3_chi_final.py        # Computes χ² and top terms
├── run_all.sh               # Script to run all jobs
├── stopwords.txt            # Stopwords list
├── data/
│   └── reviews_devset.json  # Input dataset (JSON lines)
```

## Requirements

- Python 3.11
- Virtual environment (recommended)
- mrjob

Install dependencies:
```pip install -r requirements.txt```

## How to Run
Run the full pipeline:
```
chmod +x run_all.sh
./run_all.sh 
```

### 1. Job 1: Term Counts
- Tokenizes review text
- Removes stopwords
- Counts **unique term occurrences per document and category**
Output: (term, category) → count

---

### 2. Job 2: Statistics Aggregation
Computes:
- Total documents (**N**)
- Term counts (**$N_t$**)
- Category counts (**$N_c$**)
- Joint counts (**$N_{tc}$**)

---

### 3. Job 3: Chi-Square Computation
- Computes $χ^2$ for each (term, category)
- Selects **top 75 terms per category**
- Outputs:
  - Ranked terms per category
  - Final **merged dictionary** (all unique terms sorted alphabetically)
