import argparse
from itertools import product

from tqdm import tqdm

from reddit_sentiment.annotate import process_archive
from reddit_sentiment.preprocess import BASE_DIR, ARCHIVE_REGEX


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
