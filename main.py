# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import io
import json
import pathlib
import sys
from typing import Callable

import requests
import zstandard
from textblob import TextBlob

COMMENTS_URL = "https://files.pushshift.io/reddit/comments"
ARCHIVE_TEMPLATE = "RC_{year}-{month}.zst"


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


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    year = 2005
    month = 12
    archive = ARCHIVE_TEMPLATE.format(year=year, month=month)
    version_path = pathlib.Path.cwd() / archive
    output_path = pathlib.Path.cwd() / f"{archive}.tsv"
    version_url = f"{COMMENTS_URL}/{archive}"

    if not version_path.exists():
        download_file(version_url, version_path)

    decompress_archive(version_path, output_path, process_line)
