import pandas as pd

from sklearn.metrics.pairwise import cosine_similarity

from sqlalchemy import create_engine, text

from tabulate import tabulate

# Establishing connection with SQL Database
def sqlalchemy_engine():
    server = 'DESKTOP-TOQNTIJ'
    database = 'Streaming_temp2'
    driver = 'ODBC Driver 17 for SQL Server'

    connection_string = (
        f"mssql+pyodbc://@{server}/{database}"
        f"?driver={driver}&trusted_connection=yes"
    )

    engine = create_engine(connection_string)
    print("🎉🎉 Connection established to SQL Server using SQLAlchemy 🎉🎉")
    print("*"*65)
    return engine


# For extracting and printing data from SQL Database
def extract_data(engine):
    # For user_dim table
    user_query = "SELECT * FROM user_dim;"
    user_data = pd.read_sql(user_query, con=engine)
    headers = ["User ID", "User Name", "Location", "Age Group"]
    user_table = tabulate(user_data, headers, tablefmt="grid")

    # For content_dim table
    content_query = "SELECT * FROM content_dim;"
    content_data = pd.read_sql(content_query, con=engine)
    headers = ["Content ID", "Title", "Genre", "Release Year"]
    content_table = tabulate(content_data, headers, tablefmt="grid")

    # For subscription_plan_dim table
    subscription_plan_query = "SELECT * FROM subscription_plan_dim"
    subscription_plan_data = pd.read_sql(subscription_plan_query, con=engine)
    headers = ["Plan ID", "Plan Name", "Price", "Features"]
    subscription_plan_table = tabulate(subscription_plan_data, headers, tablefmt="grid")

    # For device_dim table
    device_query = "SELECT * FROM device_dim;"
    device_data = pd.read_sql(device_query, con=engine)
    headers = ["Device ID", "Device Type", "Operating System", "Manufacturer"]
    device_table = tabulate(device_data, headers, tablefmt="grid")

    return user_table, content_table, subscription_plan_table, device_table

# For performing Extract, Transform and Load Operations (ETL)
def run_operations():
    engine = sqlalchemy_engine()

    # Extracting Data
    print("✨✨ Extracting Streaming Service Data from SQL... 👇🏻👇🏻")
    user_table, content_table, subscription_plan_table, device_table = extract_data(engine)

    print("-"*16)
    print("✨User Table: 👇🏻")
    print("-" * 16)
    print(user_table)

    print("-" * 18)
    print("✨Content Table:👇🏻")
    print("-" * 18)
    print(content_table)

    print("-" * 28)
    print("✨Subscription Plan Table:👇🏻")
    print("-" * 28)
    print(subscription_plan_table)

    print("-" * 18)
    print("✨Device Table:👇🏻")
    print("-" * 18)
    print(device_table)


if __name__ == "__main__":
    print("🙂🙂 Welcome to Data Processing and Recommendation Engine 🙂🙂")
    print("*"*65)
    run_operations()







