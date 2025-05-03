from connect import get_q

filters_sql = {
    0: """ -- posts_per_user
        SELECT p.user_id
        FROM posts p JOIN topics t ON p.topic_id = t.topic_id
        WHERE t.forum_id = %s 
          AND LENGTH(content_post) > 10 AND classification_topic >= 0.5
        GROUP BY p.user_id
        HAVING COUNT(p.post_id) > 2;
    """,
    1: """ -- unique_thread_participation_per_user
        SELECT p.user_id
        FROM posts p JOIN topics t ON p.topic_id = t.topic_id
        WHERE t.forum_id = %s
          AND LENGTH(content_post) > 10 AND classification_topic >= 0.5
        GROUP BY p.user_id
        HAVING COUNT(DISTINCT p.topic_id) > 2;
    """,
    2: """ -- posts_per_thread
        SELECT t.topic_id
        FROM topics t JOIN posts p ON t.topic_id = p.topic_id
        WHERE t.forum_id = %s
          AND LENGTH(content_post) > 10 AND classification_topic >= 0.5
        GROUP BY t.topic_id
        HAVING COUNT(p.post_id) > 2 AND COUNT(DISTINCT p.user_id) > 1;
    """,
    3: """ -- unique_users_per_thread
        SELECT t.topic_id
        FROM topics t JOIN posts p ON t.topic_id = p.topic_id
        WHERE t.forum_id = %s
          AND LENGTH(content_post) > 10 AND classification_topic >= 0.5
        GROUP BY t.topic_id
        HAVING COUNT(DISTINCT p.user_id) > 2;
    """,
}

# Filters are grouped by return type:
# Filters [0, 1] return user_ids, Filters [2, 3] return topic_ids.
# We separate them to apply intersections correctly by ID type.
def apply_filters(forum_id, active_filters):
    user_filters = []
    topic_filters = []

    for idx in active_filters:
        df = get_q(filters_sql[idx], params=(forum_id,))
        if df is None or df.empty:
            continue

        if idx in [2, 3]:
            topic_filters.append(set(df.iloc[:, 0].tolist()))
        else:
            user_filters.append(set(df.iloc[:, 0].tolist()))

    allowed_users = set.intersection(*user_filters) if user_filters else None
    allowed_topics = set.intersection(*topic_filters) if topic_filters else None
    return allowed_users, allowed_topics
