import pandas as pd

from sklearn.metrics.pairwise import cosine_similarity

from sqlalchemy import create_engine, text

from tabulate import tabulate

def sqlalchemy_engine():
    server = 'DESKTOP-TOQNTIJ'
    database = 'Streaming_temp2'
    driver = 'ODBC Driver 17 for SQL Server'

    connection_string = (
        f"mssql+pyodbc://@{server}/{database}"
        f"?driver={driver}&trusted_connection=yes"
    )

    engine = create_engine(connection_string)
    print("Connected to SQL Server with SQLAlchemy.")
    return engine

def extract_data(engine):
    user_query = "SELECT * FROM user_dim;"
    user_data = pd.read_sql(user_query, engine)
    return user_data

def run_operations():
    engine = sqlalchemy_engine()

    print("Data...")
    user_data = extract_data(engine)
    headers = ["ID", "Name", "Location", "Age Group"]
    user_table = tabulate(user_data, headers, tablefmt="grid")
    print(user_table)

if __name__ == "__main__":
    run_operations()






