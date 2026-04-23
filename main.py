import os
import argparse

from chi_square_job import ChiSquareJob


os.environ["HADOOP_STREAMING_JAR"] = "/usr/lib/hadoop/tools/lib/hadoop-streaming.jar"

def parse_arguments():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--is_local", type=str, default="true")
    parser.add_argument("--data_file_path", type=str)
    parser.add_argument("--stopwords_file_path", type=str, default="stopwords.txt")
    parser.add_argument("--output_file_path", type=str, default="output.txt")

    return parser.parse_args()


def main():
    args = parse_arguments()
    is_local = args.is_local.lower() == "true"

    # Run MapReduce chi-square computation #
    if is_local:
        job_args = [
            "-r", "local",
            "--stopwords_file_path", os.path.abspath(args.stopwords_file_path),
            os.path.abspath(args.data_file_path),
        ]
    else:
        job_args = [
            "-r", "hadoop",
            "--hadoop-streaming-jar", "/usr/lib/hadoop/tools/lib/hadoop-streaming.jar",
            "--python-bin", "/sw/venv/python312/python/bin/python",
            "--file", args.stopwords_file_path,  # Ensure the stopwords file is sent to the cluster
            "--stopwords_file_path", "stopwords.txt",
            args.data_file_path,
        ]


    # Initialize the MapReduce job with the appropriate arguments
    job = ChiSquareJob(args=job_args)

    # Run the MapReduce job and write output to output.txt
    with job.make_runner() as runner:
        runner.run()

        results = []

        for key, value in job.parse_output(runner.cat_output()):
            results.append((key, value))

        results = sorted(results, key=lambda x: x[0])

        # output_path = args.output_file_path
        with open("output.txt", "w") as f:
            for k, v in results:
                f.write(f"{k} {v}\n")

    # print(f"Output written to {output_path}")

    return


if __name__ == '__main__':
    main()
