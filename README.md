# Data-Intensive-Computing

## How to run
1. uv sync

2. python -m main --data_file_path reviews_devset.json --stopwords_file_path stopwords.txt --output_file_path output.txt

    - We may try to run this on the cluster. I have not tried it yet

## NOTE:
    - I think I finished these step:
        - Produce a file output.txt from the development set that contains the following:
        - One line for each product category (categories in alphabetic order), that contains the top 75 most discriminative terms for the category according to the chi-square test in descending order, in the following format: <category name> term_1st:chi^2_value term_2nd:chi^2_value ... term_75th:chi^2_value
    - But I dont really understand this requirement
        - One line containing the merged dictionary (all terms space-separated and ordered alphabetically)
    - We need to write a report (latex-pdf)
