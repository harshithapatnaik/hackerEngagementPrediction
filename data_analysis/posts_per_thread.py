import pandas as pd
import matplotlib.pyplot as plt
from connect import get_q

# This method counts the number of posts per thread
def plot_posts_per_thread(forum_id):
    query = """
        SELECT t.topic_id, COUNT(p.post_id) AS num_posts
        FROM topics t
        JOIN posts p ON t.topic_id = p.topic_id
        WHERE t.forum_id = %s
        AND length(content_post) > 10 AND classification_topic >= 0.5
        GROUP BY t.topic_id
        HAVING COUNT(post_id) > 2 AND COUNT(DISTINCT user_id) > 1;
    """

    df = get_q(query, params=(forum_id,))

    if df is None or df.empty:
        print("No data found or there was an error executing the query.")
        return

    # Define bins and labels for the analysis
    bins = [3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 20, 30, 50, 100, 200, 500, 1000, float('inf')]
    labels = ['3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15+', '20+', '30+', '50+', '100+', '200+', '500+', '1000+']

    # Bin the threads based on the number of posts
    df['bins'] = pd.cut(df['num_posts'], bins=bins, labels=labels, right=False)

    # Count threads per bin
    thread_counts = df['bins'].value_counts().reindex(labels).fillna(0)

    # Plotting
    plt.figure(figsize=(12, 6))
    thread_counts.plot(kind='bar', color='navy')

    plt.xlabel('Amount of total posts in thread')
    plt.ylabel('Amount of threads')
    plt.title(f'Posts per Thread in Forum {forum_id}')

    plt.grid(axis='y', linestyle='--', alpha=0.5)
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.show()

# Specify form id here
if __name__ == "__main__":
    plot_posts_per_thread(2)
