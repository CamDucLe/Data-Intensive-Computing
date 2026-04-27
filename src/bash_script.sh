IS_LOCAL=false
IS_DEV=true

STOPWORDS=data/stopwords.txt
DEV_DATA_FILE_PATH=hdfs:///dic_shared/amazon-reviews/full/reviews_devset.json
FULL_DATA_FILE_PATH=hdfs:///dic_shared/amazon-reviews/full/reviewscombined.json

echo "Current working directory: $(pwd)"

# Determine the data file path based on the environment
if [ "$IS_LOCAL" = true ]; then
    DATA_FILE_PATH=data/reviews_devset.json
    OUTPUT_FILE_PATH=data/output.txt
else
    if [ "$IS_DEV" = true ]; then
        DATA_FILE_PATH=$DEV_DATA_FILE_PATH
    else
        DATA_FILE_PATH=$FULL_DATA_FILE_PATH
    fi
    OUTPUT_FILE_PATH="$(pwd)/data/output.txt"

    # Remove the output file if it already exists
    hdfs dfs -rm -r -f $OUTPUT_FILE_PATH
fi


# Start measuring execution time
start=$(date +%s)

# Activate the virtual environment and run main script
if [ "$IS_LOCAL" = true ]; then  # Running locally
    echo "Running locally with uv"
    uv sync
    PYTHONPATH=src uv run python -m main \
    --is_local $IS_LOCAL \
    --data_file_path $DATA_FILE_PATH \
    --stopwords_file_path $STOPWORDS \
    --output_file_path $OUTPUT_FILE_PATH
else  # Running on Hadoop cluster
    PYTHONPATH=src python -m main \
    --is_local $IS_LOCAL \
    --data_file_path $DATA_FILE_PATH \
    --stopwords_file_path $STOPWORDS \
    --output_file_path $OUTPUT_FILE_PATH
fi

# End measuring execution time
end=$(date +%s)

# Calculate and display the total run time
total_seconds=$((end - start))
total_minutes=$((total_seconds / 60))
echo "Total run time: $total_seconds seconds (~ $total_minutes minutes)"
