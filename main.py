import argparse
import csv
import pathlib
import re
import subprocess
from itertools import product

import requests
from textblob import TextBlob
from tqdm.contrib.concurrent import process_map

BASE_DIR = pathlib.Path.cwd() / "dataset"
COMMENTS_URL = "https://files.pushshift.io/reddit/comments"
ARCHIVE_TEMPLATE = "RC_{year}-{month:02d}.zst"
ARCHIVE_REGEX = re.compile(r"RC_(\d{4})-(\d{2})")


def preprocess_archive(archive_url: str, archive_path: pathlib.Path) -> pathlib.Path:
    with archive_path.with_stem(archive_path.stem + "_preprocess") as f2,\
            f2.with_suffix(".tsv") as preprocessed_archive_path:
        subprocess.run([
            "./preprocess_archive.sh", archive_url,
            str(archive_path), str(preprocessed_archive_path)
        ], capture_output=True)
        return preprocessed_archive_path


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
        a_path.unlink()


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

    process_map(process_archive, sorted(year_months_to_process), max_workers=3)
