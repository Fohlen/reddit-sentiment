import argparse
import csv
from itertools import product

import requests
from textblob import TextBlob
from tqdm import tqdm

from reddit_sentiment.preprocess import BASE_DIR, COMMENTS_URL, ARCHIVE_TEMPLATE, ARCHIVE_REGEX, preprocess_archive


def url_exists(url: str) -> bool:
    response = requests.head(url)
    return response.status_code == 200


def process_archive(input_tuple: tuple[int, int]):
    year, month = input_tuple
    archive = ARCHIVE_TEMPLATE.format(year=year, month=month)
    a_path = BASE_DIR / f"{archive}"
    version_url = f"{COMMENTS_URL}/{archive}"

    if url_exists(version_url):
        pre_path = preprocess_archive(version_url, a_path)
        a_path.unlink()

        with a_path.with_suffix(".tsv") as output_path, \
                pre_path.open("rt") as fp, output_path.open("wt") as output_fp:
            reader = csv.reader(fp, delimiter="\t")

            for line in reader:
                blob = TextBlob(line[3])
                print(
                    *line[:3],
                    *blob.sentiment,
                    sep="\t",
                    file=output_fp
                )

        pre_path.unlink()


def glob_archive_year_month(pattern: str) -> set[tuple[int, int]]:
    groups = [ARCHIVE_REGEX.search(file.name).groups() for file in BASE_DIR.glob(pattern)]
    return set([(int(year), int(month)) for year, month in groups])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run sentiment analysis on reddit comments corpus.')
    parser.add_argument('start_year', nargs='?', default=2005, type=int)
    parser.add_argument('end_year', nargs='?', default=2006, type=int)
    parser.add_argument('start_month', nargs='?', default=1, type=int)
    args = parser.parse_args()
    years = list(range(args.start_year, args.end_year + 1))
    months = list(range(1, 13))

    processing_archives = glob_archive_year_month("**/RC*.zst")
    processed_archives = glob_archive_year_month("**/RC*.tsv")
    processed_archives.update(product([args.start_year], range(1, args.start_month + 1)))

    print(f"Omitting {len(processed_archives)} archives")
    product_of_years_months = set(product(years, months))
    year_months_to_process = processing_archives.union(product_of_years_months.difference(processed_archives))

    for ip in tqdm(sorted(year_months_to_process)):
        process_archive(ip)
