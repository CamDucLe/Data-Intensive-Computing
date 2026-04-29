import os
import argparse

import json
from chi_square_job import ChiSquareJob
from global_stats_job import GlobalStatsJob


# Constants for MapReduce job configuration
N_MAPPERS = 16
N_REDUCERS = 16


def parse_arguments():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--is_local", type=str, default="true")
    parser.add_argument("--data_file_path", type=str, required=True)
    parser.add_argument("--stopwords_file_path", type=str, default="data/stopwords.txt")
    parser.add_argument("--output_file_path", type=str, default="data/output.txt")

    return parser.parse_args()


def main():
    args = parse_arguments()
    is_local = args.is_local.lower() == "true"

    ##### JOB 1: Pre-compute Global Statistics (N and Nc) #####
    if is_local:
        stats_job_args = [
            "-r", "local",
            "-q",
            os.path.abspath(args.data_file_path),
        ]
    else:
        stats_job_args = [
            "-r", "hadoop",
            "-q",
            "--hadoop-streaming-jar", "/usr/lib/hadoop/tools/lib/hadoop-streaming.jar",
            "--python-bin", "/sw/venv/python312/python/bin/python",
            "--jobconf", f"mapreduce.job.maps={N_MAPPERS}",
            "--jobconf", f"mapreduce.job.reduces={N_REDUCERS}",
            args.data_file_path,
        ]

    # Run Job 1
    stats_job = GlobalStatsJob(args=stats_job_args)
    with stats_job.make_runner() as stats_runner:
        stats_runner.run()

        stats_dict = {}
        for key, value in stats_job.parse_output(stats_runner.cat_output()):
            stats_dict[key] = value

        # Write the global statistics to a JSON file to be used by the chi-square job
        with open("data/stats.json", "w") as f:
            json.dump(stats_dict, f)

    ##### JOB 2: Run MapReduce chi-square computation #####
    if is_local:
        job_args = [
            "-r", "local",
            "-q",
            "--stopwords_file_path", os.path.abspath(args.stopwords_file_path),
            "--stats_file_path", os.path.abspath("data/stats.json"),
            os.path.abspath(args.data_file_path),
        ]
    else:
        job_args = [
            "-r", "hadoop",
            "-q",
            "--hadoop-streaming-jar", "/usr/lib/hadoop/tools/lib/hadoop-streaming.jar",
            "--python-bin", "/sw/venv/python312/python/bin/python",
            "--file", args.stopwords_file_path,  # Ensure the stopwords file is sent to the cluster
            "--stopwords_file_path", "stopwords.txt",
            "--file", "data/stats.json",  # Send stats.json to cluster
            "--stats_file_path", "stats.json",
            "--jobconf", f"mapreduce.job.maps={N_MAPPERS}",
            "--jobconf", f"mapreduce.job.reduces={N_REDUCERS}",
            args.data_file_path,
        ]

    # Initialize the MapReduce job with the appropriate arguments
    job = ChiSquareJob(args=job_args)

    # Run the Job 2 and write output to output.txt
    with job.make_runner() as runner:
        runner.run()

        results = []
        all_top_terms = set()

        # Parse the output of the chi-square job
        #   which is expected to be in the format "category formatted_top_terms"
        for key, value in job.parse_output(runner.cat_output()):
            results.append((key, value))

            # Extract terms for the merged dictionary
            for item in value.split():
                term = item.split(":")[0]
                all_top_terms.add(term)

        # Sort results by category name (key) alphabetically
        results = sorted(results, key=lambda x: x[0])

        # Sort the deduplicated terms alphabetically and join with spaces
        all_top_terms = sorted(all_top_terms)
        merged_dictionary = " ".join(all_top_terms)

        output_path = args.output_file_path
        with open(output_path, "w") as f:
            for k, v in results:
                f.write(f"{k} {v}\n")

            f.write(merged_dictionary)

    return


if __name__ == '__main__':
    main()
