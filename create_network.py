from __future__ import annotations

import networkx as nx
from time import time_ns

from connect import get_q

from matplotlib import pyplot as plt
import timing


# TODO: Needs to be modified
def query_data(user_posts_threshold: int, user_threads_threshold: int, thread_posts_threshold: int,
               thread_users_threshold: int, forum_id: int):
    """
    Gets data from database and trims it based on below parameters.
    :param user_posts_threshold: The amount of posts a user must post else trimmed from dataset
                                 3 means that all users must have at least 3 total posts to be included

    :param user_threads_threshold: The amount of unique threads a user must post else be trimmed from the dataset
                                   3 means that all users must eventually participate in at least 3 unique threads to be included

    :param thread_posts_threshold: The amount of posts a thread must have else be trimmed from the dataset
                                   3 means that all threads must have at least 3 posts to be included

    :param thread_users_threshold: The amount of unique users that must be contained in a thread else be trimmed
                                   3 means that all threads must have at least 3 unique users to be included

    :param forum_id: ID of forum in focus. This model does not support multiple forums at once, as the main dataset
                     does not track users between forums.

    :returns List of all users in dataset. Also returns all relevant posts in dataset.

    """
    global start
    start = time_ns()

    get_users_query = 'SELECT p.user_id \
            FROM posts p \
            JOIN topics t ON p.topic_id = t.topic_id \
            WHERE t.forum_id = ' + str(forum_id) + ' \
            GROUP BY p.user_id \
            HAVING COUNT(p.post_id) >= ' + str(user_posts_threshold) + \
                      ' AND COUNT(DISTINCT p.topic_id) >= ' + str(user_threads_threshold) + ''
    print(get_users_query)

    users = get_q(get_users_query, 'user_id', 'posts')

    get_threads_query = 'select topic_id, post_id, user_id, dateadded_post ' \
                        'from posts ' \
                        'where topic_id in ( ' \
                        'select distinct p.topic_id ' \
                        'from posts p ' \
                        'join topics t on p.topic_id = t.topic_id ' \
                        'where t.forum_id = ' + str(forum_id) + ' and p.topic_id in ( ' \
                                                                'select distinct p.topic_id ' \
                                                                'from posts p ' \
                                                                'join topics t on p.topic_id = t.topic_id ' \
                                                                'where t.forum_id = ' + str(
        forum_id) + ' and p.user_id in (' \
                    'select p.user_id ' \
                    'from posts p ' \
                    'join topics t on p.topic_id = t.topic_id ' \
                    'where t.forum_id = ' + str(forum_id) + ' ' \
                                                            'group by p.user_id ' \
                                                            'having count(p.post_id) >= ' + str(
        user_posts_threshold) + ' and count(distinct p.topic_id) >= ' \
                        + str(user_threads_threshold) + ')' \
                                                        ') ' \
                                                        'group by p.topic_id ' \
                                                        'having count(p.post_id) >= ' + str(
        thread_posts_threshold) + ' and count(distinct p.user_id) >= ' \
                        + str(thread_users_threshold) + ' ' \
                                                        ') ' \
                                                        'order by dateadded_post asc'
    print(get_threads_query)
    users_ids = []

    # find a way to remove this. its just getting a list of user id's. All one-liners tested returned lists of lists
    # or incorrectly shaped lists.
    for index, post in users.iterrows():
        # check if user_id is in relevant users, else continue
        users_ids.append(post['user_id'])

    posts = get_q(get_threads_query, ['topic_id', 'post_id', 'user_id', 'dateadded_post'], 'posts')

    # timing.print_timing("Get from DB")
    return users_ids, posts


def create_thread_info(users, posts):
    """
    Organizes threads into dictionary with topic ID (theta) as key. Filters posts made by invalid users.
    :param users: Users that passed filtering and are to be considered in the data set.
    :param posts: All posts that are in threads that passed filtering. Post contains the user, time posted, and topic.
    :return: Thread information dictionary with key as topic id and values as every post in the thread.
    """

    # Create thread info
    global start
    start = time_ns()

    # dictionary for holding info as: key = topics_id, vals = users_id
    thread_info = {}

    # Process only the first 1000 records... just to test. remove this
    limited_posts = posts.iloc[:10]
    print(len(limited_posts))

    print(len(posts))
    # Turn loose posts into dictionary of threads with posts in order
    for index, post in posts.iterrows():
        users_id = post['user_id']
        topics_id = post['topic_id']
        posted_date = post['dateadded_post']

        # this is a filtering workaround
        # ignore nodes that are not in the initially queried users list
        if users_id not in users:
            continue

        # add thread to thread_info dict
        if topics_id not in thread_info:
            thread_info[topics_id] = []

        # add each post to thread
        # double check they are in date order?
        # constrain this by t_fos?
        thread_info[topics_id].append([users_id, posted_date])

    return thread_info


def create_network(thread_info):
    """
    Creates a network graph with thread information. Network is meant to represent whole network without time constraints.
    :param thread_info:
    :return:
    """
    g = nx.MultiDiGraph()
    # postCount = 0 #remove this line
    for topics_id in thread_info:
        # for every post in topic, when someone replies to a post, they are influence-able by everyone who posted before
        user_list = set()

        for user, date in thread_info[topics_id]:
            # print(str(topics_id) + " " + str(user) + " " + str(date))

            # add user node if not already in the graph
            if not g.has_node(user):
                g.add_node(user)

            user_list.add(user)

            for users_id in user_list:
                # prevents edges to self
                if user == users_id:
                    continue
                # edges save the difference in time between nodes with regards to a post
                # diff - diff between it and prev. Diff between new and then? Just the date?
                # date by which influence was received
                g.add_edge(users_id, user, topic=topics_id, date=date)
        #     postCount += 1 #remove this line
        # if postCount >= 20: #remove this
        #     break #remove this
        # print("im here2")
    # timing.print_timing("Collect ThreadInfo")
    print(len(g.nodes))
    if len(g.nodes) < 20:
        print("im here")
        nx.draw(g, with_labels=True)
        plt.show()
    return g

### my notes

# filter by adding classification score which should be >0.5
# SELECT p.topic_id, p.user_id
# FROM posts p
# JOIN topics t ON p.topic_id = t.topic_id
# WHERE t.classification_topic > 0.5 and t.forum_id = 2;
