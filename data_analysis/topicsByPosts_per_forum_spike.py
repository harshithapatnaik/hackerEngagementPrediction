import pandas as pd
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

# Fetch data from database
df = get_q(query)

if df is None or df.empty:
    print("No data found or there was an error executing the query.")
else:
    # Helper function to create spike data points
    def create_spikes(df, threshold_col):
        spike_data = []
        offset = 0.2  # width of spikes

        forums = df['forum_id'].tolist()
        counts = df[threshold_col].tolist()

        for forum, count in zip(forums, counts):
            if count > 0:
                spike_data.extend([
                    (forum - offset, 0),
                    (forum, count),
                    (forum + offset, 0)
                ])
            else:
                spike_data.append((forum, 0))

        return pd.DataFrame(spike_data, columns=['forum_id', threshold_col]).sort_values('forum_id')

    # Create spike DataFrames dynamically based on thresholds
    spike_dfs = [
        create_spikes(df, f'above_{threshold}')
        for threshold in THRESHOLDS
    ]

    # Plotting
    colors = ['red', 'blue', 'green', 'orange', 'purple']

    plt.figure(figsize=(14, 6))

    for spike_df, threshold, color in zip(spike_dfs, THRESHOLDS, colors):
        plt.plot(spike_df['forum_id'], spike_df[f'above_{threshold}'], label=f'Above {threshold}', color=color)

    # Labels and Title
    plt.xlabel('Forum ID')
    plt.ylabel('Number of Topics')
    plt.title('Number of Topics by Post Amount Across Forums (Spikes)')

    # Legend
    plt.legend()

    # Grid
    plt.grid(True, linestyle='--', alpha=0.5)

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