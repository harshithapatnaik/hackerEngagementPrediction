import psycopg2
import pandas as pd
import matplotlib.pyplot as plt

from connect import get_q

def plot_posts_per_user_forum(forum_id):
    query = """
        SELECT p.user_id, COUNT(p.post_id) AS num_posts
        FROM posts p
        JOIN topics t ON p.topic_id = t.topic_id
        WHERE t.forum_id = %s
        GROUP BY p.user_id;
    """

    # Load data into DataFrame
    df = get_q(query, params=(forum_id,))
    if df is None or df.empty:
        print("No data found or there was an error executing the query.")

    # Define bins for categorizing users by posts count
    bins = [1, 2, 3, 4, 5, 10, 20, 50, 100, 200, 500, 1000, float('inf')]
    labels = ['1', '2', '3', '4', '5+', '10+', '20+', '50+', '100+', '200+', '500+', '1000+']

    # Categorize data into bins
    df['bins'] = pd.cut(df['num_posts'], bins=bins, labels=labels, right=True)

    # Count users per bin and reorder correctly
    posts_per_user = df['bins'].value_counts().reindex(labels).fillna(0)

    # Plotting
    plt.figure(figsize=(12, 6))
    posts_per_user.plot(kind='bar', color='navy')

    plt.xlabel('Amount of posts')
    plt.ylabel('Amount of users')
    plt.title(f'Posts per User Distribution for Forum {forum_id}')

    plt.grid(axis='y', linestyle='--', alpha=0.5)
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.show()

# Specify forum id here
if __name__ == "__main__":
    plot_posts_per_user_forum(8)
