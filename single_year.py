import argparse

from main import process_archive

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run sentiment analysis for specific year')
    parser.add_argument('year', type=int)
    parser.add_argument('month', type=int)

    args = parser.parse_args()
    process_archive((args.year, args.month))
