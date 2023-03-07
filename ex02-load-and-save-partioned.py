#!env python
"""Load a CSV file and save it has a .parquet file partitioned by a given column"""

from argparse import ArgumentParser, FileType
from urllib.parse import quote as urlencode

import pandas as pd
import pyarrow as pa


def main():
    parser = ArgumentParser()
    parser.add_argument("input", type=FileType(mode="r"), help="Full path to an input file in CSV format")
    parser.add_argument("output", type=str, help="Full path to an output location")
    parser.add_argument("--partition-by", type=str, help="The name of a column partition up the data on saving")
    args = parser.parse_args()

    input = args.input
    records = pd.read_csv(input)
    print("Input data:")
    print(records.head(5).T)
    print()

    # create schema
    schema = pa.Schema.from_pandas(records)
    # Use smaller int for year and unsigned, default=int64
    schema = schema.set(schema.get_field_index("Year"), pa.field("Year", "uint16"))
    print("Schema:")
    print(schema)
    print()

    # save to parquet
    partition_cols = [args.partition_by] if args.partition_by is not None else None
    records.to_parquet(args.output, schema=schema, partition_cols=partition_cols)

    # load back partioned data and selected values when loading to save on memory
    filter_value = "United Kingdom"
    records_by_country = pd.read_parquet(f'{args.output}/{args.partition_by}={urlencode(filter_value)}/')
    # display ordered by Rating
    order_by = "Rating"
    print(records_by_country.sort_values(order_by, ascending=False))


if __name__ == "__main__":
    main()
