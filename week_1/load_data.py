import argparse
import os
import sys
import gzip
import shutil
from time import time
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table = params.table
    url = params.url

    # Get the file name from the URL
    file_name = url.rsplit('/', 1)[-1].strip()
    print(f'Downloading {file_name} ...')
    # Download the file from the URL
    os.system(f'curl -o {file_name} {url}')
    print('Download completed!\n')

    # Check if the file is compressed (.gz) and decompress it
    if file_name.endswith('.gz'):
        decompressed_file_name = file_name[:-3]
        print(f'Decompressing {file_name} to {decompressed_file_name}...')
        with gzip.open(file_name, 'rb') as f_in:
            with open(decompressed_file_name, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print('Decompression completed!\n')
        file_name = decompressed_file_name

    # Create SQLAlchemy engine
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Read the file based on its extension
    if file_name.endswith('.csv'):
        df = pd.read_csv(file_name, nrows=10)  # Read sample rows to infer schema
        df_iter = pd.read_csv(file_name, iterator=True, chunksize=100000)
    elif file_name.endswith('.parquet'):
        file = pq.ParquetFile(file_name)
        df = next(file.iter_batches(batch_size=10)).to_pandas()  # Read sample rows to infer schema
        df_iter = file.iter_batches(batch_size=100000)
    else:
        print('Error: Only .csv or .parquet files are supported.')
        sys.exit(1)

    # Create table in the database
    print(f'Creating table {table}...')
    df.head(0).to_sql(name=table, con=engine, if_exists='replace', index=False)
    print('Table created!\n')

    # Insert data into the table in batches
    print('Inserting data...')
    t_start = time()
    batch_count = 0

    for batch in df_iter:
        batch_count += 1
        if file_name.endswith('.parquet'):
            batch_df = batch.to_pandas()
        else:
            batch_df = batch

        print(f'Inserting batch {batch_count}...')
        b_start = time()
        batch_df.to_sql(name=table, con=engine, if_exists='append', index=False)
        b_end = time()
        print(f'Batch {batch_count} inserted in {b_end - b_start:.3f} seconds.\n')

    t_end = time()
    print(f'All data inserted! Total time: {t_end - t_start:.3f} seconds for {batch_count} batches.')


if __name__ == '__main__':
    # Parsing arguments
    parser = argparse.ArgumentParser(description='Load data from a CSV or Parquet file into a PostgreSQL database.')

    parser.add_argument('--user', required=True, help='PostgreSQL username.')
    parser.add_argument('--password', required=True, help='PostgreSQL password.')
    parser.add_argument('--host', required=True, help='PostgreSQL host.')
    parser.add_argument('--port', required=True, help='PostgreSQL port.')
    parser.add_argument('--db', required=True, help='PostgreSQL database name.')
    parser.add_argument('--table', required=True, help='Destination table name.')
    parser.add_argument('--url', required=True, help='URL of the CSV or Parquet file.')

    args = parser.parse_args()
    main(args)
