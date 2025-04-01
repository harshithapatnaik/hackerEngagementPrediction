import numpy as np
import matplotlib.pyplot as plt

from connect import get_q

# Threshold values (modify here)
THRESHOLDS = [150, 200, 300]

# Generate SQL dynamically based on thresholds
case_statements = ",\n".join([
    f"COUNT(CASE WHEN post_count > {threshold} THEN topic_id END) AS above_{threshold}"
    for threshold in THRESHOLDS
])

query = f"""
    WITH posts_per_topic AS (
        SELECT t.forum_id, t.topic_id, COUNT(p.post_id) as post_count
        FROM topics t
        JOIN posts p ON t.topic_id = p.topic_id
        GROUP BY t.forum_id, t.topic_id
    )

    SELECT forum_id,
           {case_statements}
    FROM posts_per_topic
    GROUP BY forum_id
    ORDER BY forum_id;
"""

# Fetch data using get_q function
df = get_q(query)

if df is None or df.empty:
    print("No data found or there was an error executing the query.")
else:
    # Bar width and positions
    bar_width = 0.25
    x = np.arange(len(df['forum_id']))

    # Plotting bars
    plt.figure(figsize=(16, 7))

    colors = ['red', 'blue', 'green', 'orange', 'purple']
    offset_positions = np.linspace(-bar_width, bar_width, len(THRESHOLDS))

    for offset, threshold, color in zip(offset_positions, THRESHOLDS, colors):
        plt.bar(x + offset, df[f'above_{threshold}'], width=bar_width, color=color, label=f'Above {threshold}')

    # Labels and Title
    plt.xlabel('Forum ID')
    plt.ylabel('Number of Topics')
    plt.title('Number of Topics by Post Amount Across Forums')

    # X-axis labels
    plt.xticks(x, df['forum_id'], rotation=45)

    # Legend
    plt.legend()

    # Grid
    plt.grid(axis='y', linestyle='--', alpha=0.7)

    # Tight layout
    plt.tight_layout()
    plt.show()



'''
SQL query:

WITH posts_per_topic AS (
    SELECT t.forum_id, t.topic_id, COUNT(p.post_id) as post_count
    FROM topics t
    JOIN posts p ON t.topic_id = p.topic_id
    GROUP BY t.forum_id, t.topic_id
)

SELECT forum_id,
       COUNT(CASE WHEN post_count > 60 THEN topic_id END) AS above_60,
       COUNT(CASE WHEN post_count > 120 THEN topic_id END) AS above_120,
       COUNT(CASE WHEN post_count > 180 THEN topic_id END) AS above_180
FROM posts_per_topic
GROUP BY forum_id
ORDER BY forum_id;

'''