#!/bin/bash

# Files
STOPWORDS=stopwords.txt
#INPUT=hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json
INPUT=hdfs:///dic_shared/amazon-reviews/full/reviewscombined.json
JOB1_OUT=/user/e12551603/job1_out
JOB2_OUT=/user/e12551603/job2_out
FINAL_OUT=/user/e12551603/final_output

# hdfs gives error if you try to write on an existing file
hdfs dfs -rm -r -f $JOB1_OUT 
hdfs dfs -rm -r -f $JOB2_OUT 
hdfs dfs -rm -r -f $FINAL_OUT 

echo "USING: $INPUT"
echo "Running Job 1 (term counts)"
start=$(date +%s)

python job1_term_counts.py \
    -r hadoop \
    --hadoop-streaming-jar /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
    --stopwords $STOPWORDS \
    $INPUT \
    -o hdfs://$JOB1_OUT

end=$(date +%s)
secs=$((end - start))
mins=$((secs / 60))
echo "Job 1: $secs seconds (~ $mins minutes)"

echo ""
echo "Running Job 2 (prepare aggregations)"
start=$(date +%s)

python job2_prep_aggregations.py \
    -r hadoop \
    --hadoop-streaming-jar /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
    hdfs://$JOB1_OUT \
    -o hdfs://$JOB2_OUT

end=$(date +%s)
secs=$((end - start))
mins=$((secs / 60))
echo "Job 2: $secs seconds (~ $mins minutes)"

echo ""
echo "Running Job 3 (chi-square)"
start=$(date +%s)

python job3_chi_square.py \
    -r hadoop \
    --hadoop-streaming-jar /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.6.jar \
    hdfs://$JOB2_OUT \
    -o hdfs://$FINAL_OUT

end=$(date +%s)
secs=$((end - start))
mins=$((secs / 60))
echo "Job 3: $secs seconds (~ $mins minutes)"

TOTAL_END=$(date +%s)
TOTAL_SECS=$((TOTAL_END - start))
TOTAL_MINS=$((TOTAL_SECS / 60))

echo ""
echo "Executed correctly"
echo "Total time: $TOTAL_SECS seconds (~ $TOTAL_MINS minutes)"

# To save it locally
hdfs dfs -cat /user/e12551603/final_output/part-* > final_output.txt