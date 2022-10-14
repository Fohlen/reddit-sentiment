# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import io
import json
import pathlib
import tempfile

import requests
import zstandard


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    versions = ["RC_2005-12"]
    url = "https://files.pushshift.io/reddit/comments"
    path = pathlib.Path.cwd() / f"{versions[0]}.zst"
    version_url = f"{url}/{versions[0]}.zst"

    with requests.get(version_url, stream=True) as response, path.open("wb") as fp:
        response.raise_for_status()
        for chunk in response.iter_content():
            if chunk:
                fp.write(chunk)

    with path.open("rb") as fp:
        dctx = zstandard.ZstdDecompressor(max_window_size=2147483648)
        with dctx.stream_reader(fp) as stream_reader:
            text_stream = io.TextIOWrapper(stream_reader, encoding='utf-8')
            for line in text_stream.readlines():
                print(line)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
