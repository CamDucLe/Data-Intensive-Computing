#!/bin/bash

set -e 

# Files
INPUT=data/reviews_devset.json
JOB1_OUT=out/job1.txt
JOB2_OUT=out/job2.txt
FINAL_OUT=output.txt

echo "Running Job 1 (term counts)"
python src/job1_term_counts.py $INPUT > $JOB1_OUT

echo "Running Job 2 (preparate aggregations)"
python src/job2_prep_aggregations.py $JOB1_OUT > $JOB2_OUT

echo "Running Job 3 (chi-square)"
python src/job3_chi_square.py $JOB2_OUT > $FINAL_OUT

echo "Executed correctly"