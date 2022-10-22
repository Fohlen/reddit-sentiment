reddit_sentiment
----------------

A toolkit to download, preprocess and analyse reddit sentiment data.
It is derived from the Reddit Comments Archive hosted by pushshift.

## How to run

1. Install requirements (jq, curl)
2. Create a virtual environment
3. Install the `reddit_sentiment` package
4. Download archives
5. Distill a smaller dataset

```shell
brew install curl jq
python3 -m venv venv && source venv/bin/activate
python3 setup.py build install

# use --help for help with the commands
download-annotate-archives 2005 2006 --multithreading
distill-dataset
```

## Analysis

For a basic and more advanced usages of the resulting dataset, consider the `analysis` folder.
