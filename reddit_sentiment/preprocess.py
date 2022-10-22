import pathlib
import re
import subprocess

file_path = pathlib.Path(__file__)

BASE_DIR = pathlib.Path.cwd() / "dataset"
COMMENTS_URL = "https://files.pushshift.io/reddit/comments"
ARCHIVE_TEMPLATE = "RC_{year}-{month:02d}.zst"
ARCHIVE_REGEX = re.compile(r"RC_(\d{4})-(\d{2})")
PREPROCESS_SCRIPT_PATH = file_path.parent / "preprocess_archive.sh"


def preprocess_archive(archive_url: str, archive_path: pathlib.Path) -> pathlib.Path:
    with archive_path.with_stem(archive_path.stem + "_preprocess") as f2,\
            f2.with_suffix(".tsv") as preprocessed_archive_path:
        subprocess.run([
            str(PREPROCESS_SCRIPT_PATH.absolute()), archive_url,
            str(archive_path), str(preprocessed_archive_path)
        ], capture_output=True)
        return preprocessed_archive_path
