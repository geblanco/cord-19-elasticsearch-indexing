import os
import sys
import pandas as pd
import argparse

sys.path.append(os.path.dirname(__file__))
from CSVProcessor import CSVProcessor


def fix_dates(input_csv, output_csv):
    df = pd.read_csv(input_csv)
    csv_processor = CSVProcessor()
    df["publish_time"] = df.apply(
        lambda row: csv_processor.fix_date(row['publish_time']),
        axis=1
    )
    output_dir = os.path.dirname(os.path.abspath(output_csv))
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    df.to_csv(output_csv, encoding='utf-8', index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input_csv", type=str, help="the raw csv file")
    parser.add_argument("output_csv", type=str, help="the converted csv file")
    args = parser.parse_args()

    if len(vars(args)) != 2:
        print("Wrong number of given arguments")
        sys.exit(1)

    fix_dates(args.input_csv, args.output_csv)

