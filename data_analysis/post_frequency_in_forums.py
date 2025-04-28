import psycopg2
import pandas as pd
import matplotlib.pyplot as plt

from connect import get_q

def plot_forum_post_frequency(forum_id, start_date, end_date):
    query = """
        SELECT 
            DATE_TRUNC('month', p.dateadded_post)::DATE AS month,
            COUNT(p.post_id) AS total_posts
        FROM posts p
        JOIN topics t ON p.topic_id = t.topic_id
        WHERE t.forum_id = %s
          AND p.dateadded_post >= %s
          AND p.dateadded_post <= %s
        GROUP BY month
        ORDER BY month;
    """

    # Load data into DataFrame
    df = get_q(query, params=(forum_id, start_date, end_date))

    if df is None or df.empty:
        print("No data found or there was an error executing the query.")
        return

    # Fill missing months
    df.set_index('month', inplace=True)
    date_range = pd.date_range(start=start_date, end=end_date, freq='MS')
    df = df.reindex(date_range, fill_value=0)

    # Plotting
    plt.figure(figsize=(14, 7))
    plt.plot(df.index, df['total_posts'], color='navy')

    # Titles and labels
    plt.title(f'Forum {forum_id} - Posts Frequency Over Time ({start_date} to {end_date})')
    plt.xlabel('Time (Monthly)')
    plt.ylabel('Number of Posts')

    # Formatting
    plt.grid(alpha=0.4, linestyle='--')
    plt.xticks(rotation=45)
    plt.tight_layout()

    plt.show()

# Specify forum id and dates here
if __name__ == "__main__":
    plot_forum_post_frequency(8, '2015-01-01', '2025-03-31')


'''
SQL: 

SELECT DATE_TRUNC('month', p.dateadded_post)::DATE AS month,
        COUNT(p.post_id) AS total_posts
    FROM posts p
    JOIN topics t ON p.topic_id = t.topic_id
    WHERE t.forum_id = 2
      AND p.dateadded_post >= '2015-01-01'
      AND p.dateadded_post <= '2025-03-31'
    GROUP BY month
    ORDER BY month;
        
'''