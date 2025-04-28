import pandas as pd
import matplotlib.pyplot as plt
from connect import get_q

# Counts how many distinct threads each user participates in
def plot_unique_thread_participation(forum_id):
    query = """
        SELECT p.user_id, COUNT(DISTINCT p.topic_id) AS thread_count
        FROM posts p
        JOIN topics t ON p.topic_id = t.topic_id
        WHERE t.forum_id = %s
        AND length(content_post) > 10 AND classification_topic >= 0.5
        GROUP BY p.user_id
        HAVING COUNT(DISTINCT p.topic_id) > 2;
    """

    df = get_q(query, params=(forum_id,))

    if df is None or df.empty:
        print("No data found or there was an error executing the query.")
        return

    # Define bins and labels for the analysis
    bins = [3, 4, 5, 10, 20, 50, 100, 200, 500, 1000, float('inf')]
    labels = ['3', '4', '5+', '10+', '20+', '50+', '100+', '200+', '500+', '1000+']

    # Bin the users based on unique thread participation
    df['bins'] = pd.cut(df['thread_count'], bins=bins, labels=labels, right=False)

    # Count users per bin
    user_counts = df['bins'].value_counts().reindex(labels).fillna(0)

    # Plotting
    plt.figure(figsize=(12, 6))
    user_counts.plot(kind='bar', color='navy')

    plt.xlabel('Amount of threads')
    plt.ylabel('Amount of users')
    plt.title(f'Unique thread participation by user for forum {forum_id}')

    plt.grid(axis='y', linestyle='--', alpha=0.5)
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.show()

# Specify forum id here
if __name__ == "__main__":
    plot_unique_thread_participation(11)
