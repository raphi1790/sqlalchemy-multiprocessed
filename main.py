import polars as pl
import pandas as pd
import time
import multiprocessing
from sqlalchemy import create_engine
import psycopg2


def time_logger(func):
    """Decorator to log the time taken by a function."""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        print(
            f"Function '{func.__name__}' took {time.time() - start_time} seconds.")
        return result
    return wrapper


def write_chunk_to_db(chunk, connection_string, table_name):
    """Writes a DataFrame chunk to the database."""
    try:
        engine = create_engine(connection_string)
        with engine.connect() as conn:
            chunk.to_pandas().to_sql(table_name, conn, if_exists='append', index=False)
    except Exception as e:
        print(f"Error writing chunk: {e}")


def create_table_if_not_exists(df, connection_string, table_name):
    """Creates the table if it doesn't already exist."""
    try:
        engine = create_engine(connection_string)
        with engine.connect() as conn:
            df.head(0).to_pandas().to_sql(
                table_name, conn, if_exists='replace', index=False)
            print(f"Table '{table_name}' created successfully.")
        engine.dispose()
    except Exception as e:
        print(f"Error creating table: {e}")
        raise


@time_logger
def write_dataframe_with_multiprocessing(df, connection_string, table_name, num_processes=None):
    """Writes the entire DataFrame to the database using multiprocessing."""
    if num_processes is None:
        num_processes = multiprocessing.cpu_count() - 3
        print(f"Using {num_processes} processes.")

    chunk_size = (len(df) // num_processes) + 1  # Calculate chunk size

    with multiprocessing.Pool(processes=num_processes) as pool:
        chunks = []
        for i in range(num_processes):
            start = i * chunk_size
            end = min((i + 1) * chunk_size, len(df))
            chunks.append(df[start:end].clone())

        pool.starmap(write_chunk_to_db, [
                     (chunk, connection_string, table_name) for chunk in chunks])


@time_logger
def read_dataframe_with_polars():
    csv_file = "test_data.csv"
    df = pl.read_csv(csv_file)
    return df


@time_logger
def write_dataframe_single_process(df, connection_string, table_name):
    engine = create_engine(connection_string)
    df.to_pandas().to_sql(table_name, engine, if_exists='append', index=False)
    print(
        f"Finished writing DataFrame to the database table '{table_name}' using single processing.")
    engine.dispose()


if __name__ == "__main__":
    db_connection_string = "postgresql+psycopg2://raphscho:test@localhost:5432/testdb"

    df = read_dataframe_with_polars()
    create_table_if_not_exists(
        df, db_connection_string, "single_process_table")
    write_dataframe_single_process(
        df, db_connection_string, "single_process_table")

    # # Measure time for multiprocessing
    # create_table_if_not_exists(
    #     df, db_connection_string, "multiprocessed_table")
    # write_dataframe_with_multiprocessing(
    #     df, db_connection_string, "multiprocessed_table", 5)
