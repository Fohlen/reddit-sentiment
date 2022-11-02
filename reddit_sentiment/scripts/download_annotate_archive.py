import argparse

from reddit_sentiment.annotate import process_archive

def main():
    parser = argparse.ArgumentParser(description='Run sentiment analysis for specific year')
    parser.add_argument('year', type=int)
    parser.add_argument('month', type=int)

    args = parser.parse_args()
    process_archive((args.year, args.month))


if __name__ == "__main__":
    main()
