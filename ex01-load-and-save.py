#!env python
"""Load a CSV file and save it has a .parquet file"""

from argparse import ArgumentParser, FileType
import pandas as pd
import pyarrow as pa


def main():
    parser = ArgumentParser()
    parser.add_argument("input", type=FileType(mode="r"), help="Full path to an input file in CSV format")
    parser.add_argument("output", type=str, help="Full path to an output location")
    args = parser.parse_args()

    input = args.input
    records = pd.read_csv(input)
    print("Input data:")
    print(records.head(5).T)
    print()

    # create schema
    schema = pa.Schema.from_pandas(records)
    print("Schema:")
    print(schema)
    print()

    # save to parquet
    records.to_parquet(args.output, schema=schema)


if __name__ == "__main__":
    main()
