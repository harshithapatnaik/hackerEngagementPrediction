import pandas as pd
import matplotlib.pyplot as plt
from connect import get_q

def plot_unique_users_per_thread(forum_id):
    query = """
        SELECT t.topic_id, COUNT(DISTINCT p.user_id) AS unique_users
        FROM topics t
        JOIN posts p ON t.topic_id = p.topic_id
        WHERE t.forum_id = %s
        GROUP BY t.topic_id;
    """

    df = get_q(query, params=(forum_id,))

    if df is None or df.empty:
        print("No data found or there was an error executing the query.")
        return

    # Define bins and labels for the analysis
    bins = [1, 2, 3, 4, 5, 10, 20, 50, 100, 200, 500, 1000, float('inf')]
    labels = ['1', '2', '3', '4', '5+', '10+', '20+', '50+', '100+', '200+', '500+', '1000+']

    # Bin the threads based on unique user participation
    df['bins'] = pd.cut(df['unique_users'], bins=bins, labels=labels, right=True)

    # Count threads per bin
    threads_count = df['bins'].value_counts().reindex(labels).fillna(0)

    # Plotting
    plt.figure(figsize=(12, 6))
    threads_count.plot(kind='bar', color='navy')

    plt.xlabel('Amount of unique users in thread')
    plt.ylabel('Amount of threads')
    plt.title(f'Unique User Count in Threads in Forum {forum_id}')

    plt.grid(axis='y', linestyle='--', alpha=0.5)
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.show()

# Specify forum id here
if __name__ == "__main__":
    plot_unique_users_per_thread(8)
