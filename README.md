# Playground for Apache Parquet

This repository contains sample code for interacting with the
[Apache Parquet](https://parquet.apache.org/) file format.
The [sample datasets](./datasets) are sourced from [Kaggle](https://kaggle.com).

## Getting Started

The exercises are written in Python and use Pandas for interacting with the
Parquet files.

```sh
conda env create --prefix ./condaenv --file environment.yml
conda acticate ./condaenv
```

Run basic example on the CSV data:

```sh
python ex01-load-and-save.py datasets/movies.csv /tmp/movies.parquet
```

Run partitioning example on the CSV data:

```sh
python ex02-load-and-save-partioned.py datasets/movies.csv /tmp/movies-partitioned-country.parquet --partition_by=Country_of_origin
```
