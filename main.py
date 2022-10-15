import argparse
import io
import json
import pathlib
import re
from itertools import product
from typing import Callable

from tenacity import retry
import requests
import zstandard
from textblob import TextBlob
from tqdm import tqdm

BASE_DIR = pathlib.Path.cwd() / "dataset"
COMMENTS_URL = "https://files.pushshift.io/reddit/comments"
ARCHIVE_TEMPLATE = "RC_{year}-{month:02d}.zst"
ARCHIVE_REGEX = re.compile(r"RC_(\d{4})-(\d{2})")


def url_exists(url) -> bool:
    response = requests.head(url)
    return response.status_code == 200


@retry
def download_file(url: str, file_path: pathlib.Path):
    with requests.get(url, stream=True) as response, file_path.open("wb") as fp:
        response.raise_for_status()
        for chunk in response.iter_content():
            if chunk:
                fp.write(chunk)


def decompress_archive(
        archive_path: pathlib.Path,
        output_path: pathlib.Path,
        callback: Callable[[str, io.TextIOWrapper], None]
):
    with archive_path.open("rb") as fp, output_path.open("wt") as tp:
        dctx = zstandard.ZstdDecompressor(max_window_size=2147483648)
        with dctx.stream_reader(fp) as stream_reader:
            text_stream = io.TextIOWrapper(stream_reader, encoding='utf-8')
            for line in text_stream.readlines():
                callback(line, tp)


def process_line(line: str, output: io.TextIOWrapper):
    document = json.loads(line)
    blob = TextBlob(document["body"])
    print(
        document["id"],
        document["subreddit"],
        document["created_utc"],
        *blob.sentiment,
        sep="\t",
        file=output
    )


def process_archive(year: int, month: int, unlink: bool = True):
    archive = ARCHIVE_TEMPLATE.format(year=year, month=month)
    version_path = BASE_DIR / archive
    output_path = BASE_DIR / f"{archive}.tsv"
    version_url = f"{COMMENTS_URL}/{archive}"

    if url_exists(version_url):
        if not version_path.exists():
            download_file(version_url, version_path)

        decompress_archive(version_path, output_path, process_line)

        if unlink:
            version_path.unlink()


def glob_archive_year_month(pattern: str) -> set[tuple[int, int]]:
    groups = [ARCHIVE_REGEX.search(file.name).groups() for file in BASE_DIR.glob(pattern)]
    return set([(int(year), int(month)) for year, month in groups])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run sentiment analysis on reddit comments corpus.')
    parser.add_argument('start', nargs='?', default=2006, type=int)
    parser.add_argument('end', nargs='?', default=2007, type=int)
    args = parser.parse_args()
    years = list(range(args.start, args.end + 1))
    months = list(range(1, 13))

    processing_archives = glob_archive_year_month("**/RC*.zst")
    processed_archives = glob_archive_year_month("**/RC*.zst.tsv")

    product_of_years_months = set(product(years, months))

    for y, m in tqdm(processing_archives.union(product_of_years_months.difference(processed_archives))):
        process_archive(y, m)
