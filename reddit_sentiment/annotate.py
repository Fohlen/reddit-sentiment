import csv
import requests
from textblob import TextBlob

from reddit_sentiment.preprocess import ARCHIVE_TEMPLATE, BASE_DIR, COMMENTS_URL, preprocess_archive


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