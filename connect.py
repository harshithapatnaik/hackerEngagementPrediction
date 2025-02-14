import psycopg2
import yaml
import pandas as pd

# Load configuration from config.yaml
with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

db_config = config['DATABASE']

# Function to establish a database connection
def get_db_connection():
    try:
        connection = psycopg2.connect(
            host=db_config['HOST'],
            database=db_config['DATABASE'],
            user=db_config['USER'],
            password=db_config['PASSWORD']
        )
        return connection
    except Exception as error:
        print(f"Error connecting to database: {error}")
        return None

# Function to execute a query and return a DataFrame
def get_q(query, params=None, table_name=None):
    connection = get_db_connection()
    if connection is None:
        return None

    # Adjust query if a table name or specific field is provided
    if table_name:
        query = query.replace("{table}", table_name)

    try:
        cursor = connection.cursor()
        cursor.execute(query, params)
        # Get column names from the cursor
        columns = [desc[0] for desc in cursor.description]
        result = cursor.fetchall()
        # Convert the result to a pandas DataFrame
        df = pd.DataFrame(result, columns=columns)
        return df
    except Exception as error:
        print(f"Error executing query: {error}")
        return None
    finally:
        if connection:
            cursor.close()
            connection.close()

# Function for executing simple SELECT queries with optional WHERE clause
def get(table, fields="*", where=None):
    query = f"SELECT {fields} FROM {table}"
    if where:
        query += f" WHERE {where}"

    return get_q(query)

# Test Database Connection
if __name__ == "__main__":
    # Example usage of get_q
    query = "SELECT * FROM users;"
    users_df = get_q(query)
    if users_df is not None:
        print(users_df)

    # Example usage of get function with WHERE clause
    queryF = "SELECT p.topic_id, p.user_id FROM posts p JOIN topics t ON p.topic_id = t.topic_id WHERE t.forum_id = 2;"
    posts_df = get_q(queryF)
    if posts_df is not None:
        print(posts_df)

    # Establish a database connection directly (instead of using get for 'db_connection')
    connection = get_db_connection()
    if connection:
        print("Connection established using 'get_db_connection' function.")
        connection.close()
