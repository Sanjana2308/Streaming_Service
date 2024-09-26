import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
from sqlalchemy import create_engine
from tabulate import tabulate
from sklearn.metrics import mean_squared_error


# ------Establishing connection with SQL Database------
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
    print("*" * 65)
    return engine


# ------For extracting and printing data from SQL Database------
def extract_data(engine):
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

# ------Formatting extracted data------
def formatting_extracted_data(user_data, content_data, subscription_plan_data, device_data, interaction_data):
    # Users table
    headers = ["User ID", "User Name", "Location", "Age Group"]
    user_table = tabulate(user_data, headers, tablefmt="grid")
    print("-" * 16)
    print("✨User Table: 👇🏻")
    print("-" * 16)
    print(user_table)

    # Content Table
    headers = ["Content ID", "Title", "Genre", "Release Year"]
    content_table = tabulate(content_data, headers, tablefmt="grid")
    print("-" * 18)
    print("✨Content Table:👇🏻")
    print("-" * 18)
    print(content_table)

    # Subscription Plan table
    headers = ["Plan ID", "Plan Name", "Price", "Features"]
    subscription_plan_table = tabulate(subscription_plan_data, headers, tablefmt="grid")
    print("-" * 28)
    print("✨Subscription Plan Table:👇🏻")
    print("-" * 28)
    print(subscription_plan_table)

    # Device Table
    headers = ["Device ID", "Device Type", "Operating System", "Manufacturer"]
    device_table = tabulate(device_data, headers, tablefmt="grid")
    print("-" * 18)
    print("✨Device Table:👇🏻")
    print("-" * 18)
    print(device_table)


    # Interaction Table
    headers = ["Interaction Id", "User ID", "Content ID", "Plan ID", "Device ID", "Watch Time",
                       "Rating", "Activity Type", "Activity Timestamp", "Interaction Date"]
    interaction_table = tabulate(interaction_data, headers, tablefmt="grid")
    print("-" * 23)
    print("✨Interaction Table: 👇🏻")
    print("-" * 23)
    print(interaction_table)


# ------To build recommendation engine------
def recommendation_engine(target_user, interaction_data, content_data):

    # ----Collaborative Filtering----
    user_content_matrix = interaction_data.pivot_table(index='user_id',
                                                        columns='content_id',
                                                        values='rating').fillna(0)

    user_similarity = cosine_similarity(user_content_matrix)
    # Get the similarity score of the target user with other users
    user_sim_scores = user_similarity[user_content_matrix.index == target_user].flatten()

    # Predict ratings by taking weighted average of similar users' ratings
    predicted_ratings = user_sim_scores.dot(user_content_matrix) / user_sim_scores.sum()

    # ----Content based Filtering----
    # Create a matrix of content features for similarity calculation
    content_features_matrix = content_data[['genre', 'release_year']]  # You can add more features

    # Use one-hot encoding for categorical features like genre
    content_features_matrix = pd.get_dummies(content_features_matrix)

    content_similarity = cosine_similarity(content_features_matrix)

    # Fetch user's watched or highly rated content
    user_ratings = user_content_matrix.loc[target_user]

    liked_content = user_ratings[user_ratings > 3]

    # Initialize a full-length series with float type to avoid dtype mismatch
    liked_content_full = pd.Series([0.0] * content_similarity.shape[0], index=user_content_matrix.columns)

    # Update liked_content_full with the user's liked content ratings
    liked_content_full.update(liked_content.astype(float))

    # Recommend similar content to the content the user liked
    similar_content_scores = content_similarity.dot(liked_content_full)

    recommended_content_indices = similar_content_scores.argsort()[-5:][::-1]
    recommended_content = content_data.iloc[recommended_content_indices]
    print("✨✨ Recommended content for you 👇👇")

    print("-" * 52)
    print(recommended_content[['title', 'genre']])

    print("*" * 52)

    return user_content_matrix, predicted_ratings


# ------Evaluation of Recommendation------
def evaluate_recommendation(target_user, user_content_matrix, predicted_ratings):
    true_ratings = user_content_matrix.loc[target_user]
    mse = mean_squared_error(true_ratings, predicted_ratings)

    print("-" * 55)
    print(f"✨ Mean Squared Error (MSE) for recommendations 👉 {mse}")
    print("-" * 55)

    if mse == 0:
        print("🎉🎉 Recommended content is 'Perfectly Accurate' 🎉🎉")
    elif mse > 1:
        print("😢😢 Recommended content is 'Not Accurate' 😢😢")
    else:
        print("😐😐 Recommended content is 'Partially Accurate' 😐😐")




# ------For performing Extract, Transform and Recommendation Operations------
def run_operations():
    engine = sqlalchemy_engine()


    print("\n\n")
    print("*" * 170)

    # Extract data
    print("✨✨ Extracting Streaming Service Data from SQL... 👇🏻👇🏻")
    user_data, content_data, subscription_plan_data, device_data, interaction_data = extract_data(engine)
    formatting_extracted_data(user_data, content_data, subscription_plan_data, device_data, interaction_data)

    print("*" * 170)
    print("\n\n")
    print("*" * 52)

    # Recommending content to users
    print("😊😊 To view Recommended Content enter User ID 😊😊")
    print("-" * 52)

    user_id = int(input("✨ Enter user id: "))

    print("-" * 52)

    user_content_matrix, predicted_ratings = recommendation_engine(user_id, interaction_data, content_data)


    print("\n\n")
    print("*" * 55)

    # Evaluating Recommendation for User
    print("✨✨ Evaluating Recommendations Accuracy... ✨✨")

    evaluate_recommendation(user_id, user_content_matrix, predicted_ratings)

    print("*" * 55)
    print("✨" * 25)
    print("T      H      A      N      K          Y      O      U")
    print("✨" * 25)


if __name__ == "__main__":
    print("*" * 65)
    print("🙂🙂 Welcome to Data Processing and Recommendation Engine 🙂🙂")
    print("*"*65)
    run_operations()







