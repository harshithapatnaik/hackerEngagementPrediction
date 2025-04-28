from connect import get_q
from collections import defaultdict
import networkx as nx
import matplotlib.pyplot as plt
import random


# Build thread info structure
def build_thread_info(forum_id, allowed_users=None, allowed_topics=None):
    query = """
        SELECT p.post_id, p.topic_id, p.user_id, p.dateadded_post
        FROM posts p
        JOIN topics t ON p.topic_id = t.topic_id
        WHERE t.forum_id = %s
          AND LENGTH(p.content_post) > 10
          AND t.classification_topic >= 0.5
        ORDER BY p.topic_id, p.dateadded_post;
    """

    df = get_q(query, params=(forum_id,))
    if df is None or df.empty:
        print("No data found.")
        return {}

    thread_info = defaultdict(list)

    for _, row in df.iterrows():
        topic_id = row['topic_id']
        user_id = row['user_id']
        post_id = row['post_id']
        post_time = row['dateadded_post']

        # Apply filters if provided
        if allowed_topics and topic_id not in allowed_topics:
            continue
        if allowed_users and user_id not in allowed_users:
            continue

        thread_info[topic_id].append((post_id, user_id, post_time))

    return dict(thread_info)


# Create influence graph
def create_user_influence_network(thread_info):
    """
    Create a directed influence graph from thread info.
    An edge is created from every user who posted before another in the same thread.
    Each edge includes the topic ID, timestamp, post ID, and user ID of the influencer.
    """

    g = nx.MultiDiGraph()

    for topic_id, posts in thread_info.items():
        posts = sorted(posts, key=lambda x: x[2])  # sort by timestamp (index 2)

        for i in range(len(posts)):
            post_id_i, user_i, time_i = posts[i]
            g.add_node(user_i)

            for j in range(i):
                post_id_j, user_j, time_j = posts[j]
                if user_i != user_j:
                    g.add_edge(
                        user_j, user_i,
                        topic=topic_id,
                        date=time_j,
                        from_post=post_id_j,
                        from_user=user_j
                    )

    print(f"Graph built: {g.number_of_nodes()} users, {g.number_of_edges()} connections.")

    if len(g.nodes) <= 20:
        nx.draw(g, with_labels=True, node_size=600, font_size=8)
        plt.title("Influence Graph")
        plt.show()

    return g


def print_full_network(graph):
    """
    Print all edges in the influence network including post ID, user ID, timestamp, and thread.
    """
    print("=== Influence Network Edges ===")
    for u, v, edge_data in graph.edges(data=True):
        topic = edge_data.get('topic', 'N/A')
        time = edge_data.get('date', 'N/A')
        from_post = edge_data.get('from_post', 'N/A')
        from_user = edge_data.get('from_user', u)
        print(f"Post {from_post} by User {from_user} → User {v} "
              f"(Thread: {topic}, Time: {time})")

if __name__ == "__main__":
    forum_id = 4
    thread_info = build_thread_info(forum_id)
    graph = create_user_influence_network(thread_info)
    print_full_network(graph)
