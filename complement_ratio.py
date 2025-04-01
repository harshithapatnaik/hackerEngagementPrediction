from create_network import query_data, create_network, create_thread_info
import config
from datetime import timedelta
from tqdm import tqdm
import getFeatures as gf

# Get forum config
forum_config = config.get_config(config, "FORUM")
forum_id = forum_config.get("ID")
forum_post_threshold = forum_config.get("POST_THRESHOLD")
date_config = config.get_config(config, "DATE")
date_begin = date_config.get("BEGIN")
date_end = date_config.get("END")

# Get network config
network_config = config.get_config(config, "NETWORK")
user_post_requirement = network_config.get("USER_POSTS_THRESHOLD")
user_thread_requirement = network_config.get("USER_THREADS_THRESHOLD")
thread_post_requirement = network_config.get("THREAD_POSTS_THRESHOLD")
thread_users_requirement = network_config.get("THREAD_USERS_THRESHOLD")

# Get tao config
t_config = config.get_config(config, "TAO")
t_sus = timedelta(hours=int(t_config.get("SUSCEPTIBLE")))
t_fos = timedelta(hours=int(t_config.get("FORGETTABLE")))

# Get feature config
feature_config = config.get_config(config, "FEATURE")

users, posts = query_data(user_post_requirement,
                          user_thread_requirement,
                          thread_post_requirement,
                          thread_users_requirement,
                          forum_id)

thread_info = create_thread_info(users, posts)
net = create_network(thread_info)

total_average = 0.0
total_count = 0

for thread in tqdm(thread_info):
    thread_posts = thread_info[thread]
    prev_posts = []
    active_users = set()
    average = 0.0
    for user, time in thread_posts:
        active_users.add(user)
    out_neighbors_thread = set()
    for user in active_users:
        # can't have active neighbors without previous posts
        out_neighbors = gf.get_out_neighbors(net.out_edges(user))
        if out_neighbors:
            out_neighbors_thread = out_neighbors | out_neighbors_thread
    if len(active_users) > 1 and len(out_neighbors_thread) > 1:
        total_average += (len(active_users) / len(out_neighbors_thread))
        total_count += 1
print(total_average / total_count)
print(total_average / total_count)
