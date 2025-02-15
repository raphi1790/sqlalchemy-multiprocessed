import polars as pl
import time


def time_logger(func):
    """Decorator to log the time taken by a function."""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        print(
            f"Function '{func.__name__}' took {time.time() - start_time} seconds.")
        return result
    return wrapper


 # Konfiguration
# Ersetzen Sie dies durch den tatsächlichen Pfad zu Ihrer CSV-Datei
CSV_DATEIPFAD = "test_data.csv"
# Ersetzen Sie dies durch den gewünschten Namen für Ihre Tabelle
TABELLEN_NAME = "polars_test"

# Datenbankverbindung konfigurieren (ersetzen Sie die Platzhalter)
DB_USER = "raphscho"
DB_PASSWORD = "test"
DB_HOST = "localhost"
DB_NAME = "testdb"


DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"


@time_logger
def read_dataframe_with_polars():
    df = pl.read_csv(CSV_DATEIPFAD)
    return df


@time_logger
def write_dataframe(df, connection_string, table_name):
    df.write_database(
        table_name=table_name,
        connection=connection_string,
        engine="adbc",
    )


# Multiprocessing verwenden, um das Schreiben zu beschleunigen
if __name__ == "__main__":
    df = read_dataframe_with_polars()
    write_dataframe(df, DB_URL, TABELLEN_NAME)
    print("CSV-Datei erfolgreich in die Postgres-Datenbank geschrieben.")
