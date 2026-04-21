import os
import argparse


from chi_square_job import ChiSquareJob


def parse_arguments():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data_file_path",
        type=str,
        default="reviews_devset.json",
    )
    parser.add_argument(
        "--stopwords_file_path",
        type=str,
        default="stopwords.txt",
    )
    parser.add_argument(
        "--output_file_path",
        type=str,
        default="output.txt",
    )

    return parser.parse_args()


def main():
    args = parse_arguments()

    # Run MapReduce chi-square computation #
    job_args = [
        "-r", "local",
        "--stopwords_file_path", os.path.abspath(args.stopwords_file_path),
        os.path.abspath(args.data_file_path),
    ]
    job = ChiSquareJob(args=job_args)

    # Run the MapReduce job and write output to output.txt
    with job.make_runner() as runner:
        runner.run()

        results = list(job.parse_output(runner.cat_output()))
        results = sorted(results, key=lambda x: x[0])  # type: ignore

        with open(os.path.abspath(args.output_file_path), "w") as f:
            for key, value in results:
                f.write(f"{key} {value}\n")

        print(f"Output written to {os.path.abspath(args.output_file_path)}")

    return


if __name__ == '__main__':
    main()
