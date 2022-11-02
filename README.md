reddit_sentiment
----------------

A toolkit to download, preprocess and analyse reddit sentiment data.
It is derived from the Reddit Comments Archive hosted by pushshift.

## How to run

1. Install requirements (jq, curl), e.g `brew install jq curl`
2. [Install Poetry](https://python-poetry.org/docs/#installation)
5. Download archives
6. Distill a smaller dataset

```shell
poetry install

# use --help for help with the commands
poetry run download-annotate-archives 2005 2006 --multithreading
poetry run distill-dataset
```

## Analysis

For a basic and more advanced usages of the resulting dataset, consider the `analysis` folder.
