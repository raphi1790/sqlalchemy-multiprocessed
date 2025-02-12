import pandas as pd
import time
import multiprocessing
from sqlalchemy import create_engine


def write_chunk_to_db(chunk, connection_string, table_name):
    engine = create_engine(connection_string)
    chunk.to_sql(table_name, engine, if_exists='append', index=False)


def write_dataframe_with_multiprocessing(df, connection_string, table_name, num_processes=None):
    """Writes the entire DataFrame to the database using multiprocessing."""

    if num_processes is None:
        num_processes = multiprocessing.cpu_count()

    chunk_size = (len(df) // num_processes) + 1  # Calculate chunk size

    processes = []
    for i in range(num_processes):
        start = i * chunk_size
        end = min((i + 1) * chunk_size, len(df))
        # Create a copy of the dataframe chunk
        chunk_to_process = df.iloc[start:end].copy()
        p = multiprocessing.Process(target=write_chunk_to_db, args=(
            chunk_to_process, connection_string, table_name))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    print(
        f"Finished writing DataFrame to the database table '{table_name}' using multiprocessing.")


def write_dataframe_single_process(df, connection_string, table_name):
    engine = create_engine(connection_string)
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(
        f"Finished writing DataFrame to the database table '{table_name}' using single processing.")


if __name__ == "__main__":
    csv_file = "test_data.csv"
    db_connection_string = "postgresql+psycopg2://raphscho:test@localhost:5432/testdb"

    try:
        # Read the entire CSV
        df = pd.read_csv(csv_file)

        # Measure time for single processing
        start_time = time.time()
        write_dataframe_single_process(
            df, db_connection_string, "single_process_table")
        single_process_time = time.time() - start_time
        print(f"Single processing time: {single_process_time} seconds")

        # Measure time for multiprocessing
        start_time = time.time()
        write_dataframe_with_multiprocessing(
            df, db_connection_string, "multiprocessed_table")
        multi_process_time = time.time() - start_time
        print(f"Multiprocessing time: {multi_process_time} seconds")

    except FileNotFoundError:
        print(f"Error: CSV file '{csv_file}' not found.")
    except pd.errors.EmptyDataError:
        print(f"Error: CSV file '{csv_file}' is empty.")
    except Exception as e:
        print(f"An error occurred: {e}")
