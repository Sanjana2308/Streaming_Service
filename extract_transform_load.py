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
def extract_data_from_sql(engine):
    # For user_dim table
    user_query = "SELECT * FROM user_dim;"
    user_data = pd.read_sql(user_query, con=engine)


    # For content_dim table
    content_query = "SELECT * FROM content_dim;"
    content_data = pd.read_sql(content_query, con=engine)


    # For subscription_plan_dim table
    subscription_plan_query = "SELECT * FROM subscription_plan_dim"
    subscription_plan_data = pd.read_sql(subscription_plan_query, con=engine)

    # For device_dim table
    device_query = "SELECT * FROM device_dim;"
    device_data = pd.read_sql(device_query, con=engine)


    unified_interaction_query = "SELECT * FROM unified_interaction_fact;"
    interaction_data = pd.read_sql(unified_interaction_query, con=engine)

    return user_data, content_data, subscription_plan_data, device_data, interaction_data

def extract(user_data, content_data, subscription_plan_data, device_data, interaction_data):

    while True:
        print("Choose from the below list 👇🏻: ")
        print("""1. Users Data
2. Content Data
3. Subscription Plan Data
4. Device Data
5. User Interaction Data
6. Back to main menu
                        """)

        choice = int(input("Enter type of data to be extracted: "))
        if choice == 1:
            # Users table
            headers = ["User ID", "User Name", "Location", "Age Group"]
            user_table = tabulate(user_data, headers, tablefmt="grid")
            print("-" * 16)
            print("✨User Table: 👇🏻")
            print("-" * 16)
            print(user_table)
            break

        elif choice == 2:
            # Content Table
            headers = ["Content ID", "Title", "Genre", "Release Year"]
            content_table = tabulate(content_data, headers, tablefmt="grid")
            print("-" * 18)
            print("✨Content Table:👇🏻")
            print("-" * 18)
            print(content_table)
            break

        elif choice == 3:
            # Subscription Plan table
            headers = ["Plan ID", "Plan Name", "Price", "Features"]
            subscription_plan_table = tabulate(subscription_plan_data, headers, tablefmt="grid")
            print("-" * 28)
            print("✨Subscription Plan Table:👇🏻")
            print("-" * 28)
            print(subscription_plan_table)
            break

        elif choice == 4:
            # Device Table
            headers = ["Device ID", "Device Type", "Operating System", "Manufacturer"]
            device_table = tabulate(device_data, headers, tablefmt="grid")
            print("-" * 18)
            print("✨Device Table:👇🏻")
            print("-" * 18)
            print(device_table)
            break

        elif choice == 5:
            # Interaction Table
            headers = ["Interaction Id", "User ID", "Content ID", "Plan ID", "Device ID", "Watch Time",
                       "Rating", "Activity Type", "Activity Timestamp", "Interaction Date"]
            interaction_table = tabulate(interaction_data, headers, tablefmt="grid")
            print("-" * 23)
            print("✨Interaction Table: 👇🏻")
            print("-" * 23)
            print(interaction_table)
            break

        elif choice == 6:
            return

# To build collaborative filtering
def collaborative_filtering(engine, user_id, interaction_data):

    content_query = "SELECT content_id FROM content_dim"
    content_id_data = pd.read_sql(content_query, con=engine)
    print(content_id_data)

    # Create user-content interaction matrix for collaborative filtering
    user_content_matrix = interaction_data.pivot_table(index='user_id', columns='content_id', values='rating').fillna(0)

    # Compute cosine similarity between users
    user_similarity = cosine_similarity(user_content_matrix)

    # Select user for whom to make recommendations
    target_user = user_id
    # Get the similarity score of the target user with other users
    user_sim_scores = user_similarity[target_user]
    # Predict ratings by taking weighted average of similar users' ratings
    predicted_ratings = user_sim_scores.dot(user_content_matrix) / user_sim_scores.sum()
    return predicted_ratings

def content_based_filtering():
    pass


# For performing Extract, Transform and Recommendation Operations
def run_operations():
    engine = sqlalchemy_engine()

    print("✨✨ Extracting Streaming Service Data from SQL... 👇🏻👇🏻")
    user_data, content_data, subscription_plan_data, device_data, interaction_data = extract_data_from_sql(engine)

    while True:
        print("\n👇🏻 Choose functionality for Recommendation Engine: ")
        print("""1. Extract Data
2. Predict ratings for unrated content 
3. Recommend content for the user
4. Evaluating Recommendations
5. Exit
        """)

        choice = int(input("Enter your choice 😃: "))

        while True:
            if choice == 1:
                extract(user_data, content_data, subscription_plan_data, device_data, interaction_data)
                break

            elif choice == 2:
                user_id = int(input("Enter User ID: "))

                print("Predicted Ratings for content they have not rated: ")
                predicted_ratings = collaborative_filtering(engine, user_id, interaction_data)
                print(predicted_ratings)
                break

            elif choice == 3:
                user_id = int(input("Enter User ID: "))
                print("Recommended Content for User ID: ",user_id)

                break

            elif choice == 4:
                pass





















    #


if __name__ == "__main__":
    print("🙂🙂 Welcome to Data Processing and Recommendation Engine 🙂🙂")
    print("*"*65)
    run_operations()







