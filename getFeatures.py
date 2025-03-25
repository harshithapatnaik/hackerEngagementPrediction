import pandas as pd
from time import time_ns
import timing
from tqdm import tqdm
from random import shuffle


# TODO: Put timing in this!!!
# TODO: Use ordered list for logn times

# note that neighbors for sake of this research are in-neighbors only.

# 1. number of active neighboors

# 2. Personal Network Exposure
# "This  value  is defined as the ratio of number of active neighbors to
# total number  of  neighbors. (An Empirical Evaluation of Social Influence Metrics

# 3. Average in neighbor count of active neighbors (we SHOULD check this... see if theres a correlation
#    AIC
# 4. Average out neighbor count of active neighbors (Why not?)


def get_active_neighbors(prev_posts, neighbors, t, t_fos):
    active_neighbors = set()
    t_fos = t - t_fos
    for post in prev_posts:
        user = post[0]
        date = post[1]
        if user in neighbors and t >= date > t_fos:
            active_neighbors.add(user)
    return active_neighbors


def get_NAN(prev_posts, neighbors, t, t_fos):
    active_neighbors = get_active_neighbors(prev_posts, neighbors, t, t_fos)
    return len(active_neighbors)


def get_active_neighbors_and_hubs(prev_posts, in_neighbors, t, t_sus, t_fos, net):
    active_neighbors = set()
    hubs_count = 0
    t_fos = t - t_fos
    for user, date in prev_posts:
        if user in in_neighbors and t >= date > t_fos:
            if user not in active_neighbors:
                active_neighbors.add(user)
                if len(get_out_neighbors_at_time(net.out_edges(user), t, t_sus, net)) > 50:
                    hubs_count += 1
    return active_neighbors, hubs_count


def get_NAN_and_HUB(prev_posts, in_neighbors, t, t_sus, t_fos, net):
    active_neighbors, hubs = get_active_neighbors_and_hubs(prev_posts, in_neighbors, t, t_sus, t_fos, net)
    return active_neighbors, len(active_neighbors), hubs


def get_active_neighbors_and_NAN(prev_posts, neighbors, t, t_fos):
    active_neighbors = get_active_neighbors(prev_posts, neighbors, t, t_fos)
    return active_neighbors, len(active_neighbors)


# TODO: Make the hub threshold configurable from a higher level
def get_HUB(active_neighbors, t, t_sus, net):
    hub_count = 0
    for neighbor in active_neighbors:
        out_count = get_out_neighbors_at_time(net.out_edges(neighbor), t, t_sus, net)
        if len(out_count) > 50:
            hub_count += 1
    return hub_count


def get_f1(positive_users, user, net):
    # Return who in network is active
    neighbors = net.in_edges(user)
    active_neighbors = 0
    for n in neighbors:
        if n in positive_users:
            active_neighbors += 1
    return active_neighbors


def get_PNE(NAN, neighbors):
    if neighbors == 0:
        return 0
    return NAN / neighbors


# Return PNE - active neighbor count over total number of users.
# Counting self user as active neighbor?
def get_f2(active_neighbors, user, net):
    x = net.in_degree(user)
    if x == 0:
        return 0
    else:
        return active_neighbors / x


def get_f3(users, net):  # G.out_degree(1) average
    summation = 0
    for usr in users:
        summation += (net.in_degree(usr))
    return summation / len(users)


def get_in_neighbors_at_time(in_edges, t, t_sus, net):
    # don't look at edges after t (in the future)
    t_sus = t - t_sus
    neighbors = set()
    for neighbor, user in in_edges:
        data = net.get_edge_data(neighbor, user)
        for i in data:
            # prob dont need this if statement
            if data:
                date = data.get(i)['date']
                if t >= date > t_sus:
                    neighbors.add(neighbor)
                    break
    return neighbors


def get_in_prev_post_count_at_time(in_edges, t, t_sus, net):
    # don't look at edges after t (in the future)
    t_sus = t - t_sus
    posts = 0
    for neighbor, user in in_edges:
        data = net.get_edge_data(neighbor, user)
        for i in data:
            # prob dont need this if statement
            if data:
                date = data.get(i)['date']
                if t >= date > t_sus:
                    posts += 1
                    break
    return posts


def get_out_neighbors_at_time(out_edges, t, t_sus, net):
    # don't look at edges after t (in the future)
    t_sus = t - t_sus
    neighbors = set()
    if out_edges:
        for user, neighbor in out_edges:
            data = net.get_edge_data(user, neighbor)
            for i in data:
                # prob dont need this if statement
                if data:
                    date = data.get(i)['date']
                    if t >= date > t_sus:
                        neighbors.add(neighbor)
                        break
    return neighbors


def get_out_neighbors(out_edges):
    neighbors = set()
    for user, neighbor in out_edges:
        neighbors.add(neighbor)
    return neighbors


def get_root_user(prev_posts, t, t_fos):
    t_fos = t - t_fos
    for user, date in prev_posts:
        if t >= date > t_fos:
            return user
    return None


def get_negative_user(pos_user, prev_posts, prev_posters, root_neighbors, t, t_sus, t_fos, net, in_dataset_negative):
    for user in root_neighbors:
        if user in prev_posts or user in in_dataset_negative:
            continue
        else:
            in_neighbors = get_in_neighbors_at_time(net.in_edges(user), t, t_sus, net)
            if in_neighbors:
                active_neighbors = get_active_neighbors(prev_posts, in_neighbors, t, t_fos)
                if pos_user in active_neighbors:
                    continue
                if len(active_neighbors) >= 1 and user not in prev_posters:
                    return user, in_neighbors, len(active_neighbors)
    return None, None, None


def get_balanced_dataset(thread_list, thread_info, N, t_sus, t_fos, features_bits, positive_users):
    global start
    start = time_ns()

    print(f"Number of threads in thread_list: {len(thread_list)}")
    print(f"Number of threads in thread_info: {len(thread_info)}")
    print(f"Number of nodes in network (N): {len(N.nodes)}")
    print(f"Number of features in features_bits: {len(features_bits)}")
    print(f"Number of positive users per thread: {[len(positive_users[thread]) for thread in thread_list]}")

    net = N
    data = []

    for thread in tqdm(thread_list):
        thread_posts = thread_info[thread]
        prev_posts = []
        prev_posters = set()
        active_users_total = positive_users[thread]
        in_dataset = set()
        in_dataset_negative = set()

        for user, time in thread_posts:
            prev_posts.append((user, time))
            prev_posters.add(user)
            if len(prev_posters) == len(active_users_total):
                break
            # can't have active neighbors without previous posts
            if len(prev_posts) > 1:
                if user in in_dataset or time < time - t_fos:
                    continue

                in_neighbors = get_in_neighbors_at_time(net.in_edges(user), time, t_sus, net)

                if features_bits[2]:
                    active_neighbors, NAN, HUB = get_NAN_and_HUB(prev_posts, in_neighbors, time, t_sus, t_fos, net)
                else:
                    NAN = get_NAN(prev_posts, in_neighbors, time, t_fos)

                # Check if NAN condition filters out the user
                # if NAN < 1:
                #     print(f"User {user} was skipped due to NAN < 1")
                #     in_dataset.add(user)  # Count them as added without adding them... they're an "innovator"
                #     continue
                if NAN == 0:
                    print(f"User {user} was skipped due to NAN == 0")
                    in_dataset.add(user)
                    continue

                if features_bits[1]:
                    PNE = get_PNE(NAN, len(in_neighbors))

                if features_bits[3]:
                    PPP = get_in_prev_post_count_at_time(net.in_edges(user), time, t_sus, net)

                # make negative sample
                root_user = get_root_user(prev_posts, time, t_fos)
                if not root_user:
                    continue

                root_neighbors = get_out_neighbors_at_time(net.out_edges(root_user), time, t_sus, net)

                negative_user, in_neighbors_negative, NAN_negative = get_negative_user(
                    user, prev_posts, prev_posters, root_neighbors, time, t_sus, t_fos, net, in_dataset_negative
                )
                if not negative_user:
                    continue

                if features_bits[2]:
                    negative_active_neighbors, NAN_negative, HUB_negative = get_NAN_and_HUB(
                        prev_posts, in_neighbors_negative, time, t_sus, t_fos, net
                    )

                # Check if NAN condition filters out the negative user
                if NAN_negative < 1:
                    print(f"Negative user {negative_user} was skipped due to NAN_negative < 1")
                    continue

                if features_bits[1]:
                    PNE_negative = get_PNE(NAN_negative, len(in_neighbors_negative))

                # Append positive and negative samples
                data_row = [user]
                if features_bits[0]:
                    data_row.append(NAN)
                if features_bits[1]:
                    data_row.append(PNE)
                if features_bits[2]:
                    data_row.append(HUB)
                if features_bits[3]:
                    data_row.append(PPP)
                data_row.append(1)
                data.append(data_row)

                negative_data_row = [negative_user]
                if features_bits[0]:
                    negative_data_row.append(NAN_negative)
                if features_bits[1]:
                    negative_data_row.append(PNE_negative)
                if features_bits[2]:
                    negative_data_row.append(HUB_negative)
                # if features_bits[3]:
                #     negative_data_row.append(PPP_negative)
                negative_data_row.append(0)
                in_dataset_negative.add(negative_user)
                data.append(negative_data_row)

    # After data collection, print the size of the dataset
    print(f"Total number of records collected: {len(data)}")

    if not data:
        print("No data was collected in get_balanced_dataset. Please review conditions.")

    columns = ['user_id']
    data_message = "Compiled Data: "
    if features_bits[0]:
        columns.append('NAN')
        data_message += "NAN "
    if features_bits[1]:
        columns.append('PNE')
        data_message += "PNE "
    if features_bits[2]:
        columns.append('HUB')
        data_message += "HUB "
    if features_bits[3]:
        columns.append('PPP')
        data_message += "PPP "

    columns.append('Class')
    df = pd.DataFrame(data, columns=columns)
    timing.print_timing(data_message)
    return df



def get_ratio(thread_info, net, t_sus, t_fos, user_amount):
    global start
    start = time_ns()
    ratio = 0.0
    count = 0

    for thread in thread_info:
        thread_posts = thread_info[thread]
        prev_posts = []
        prev_posters = set()
        in_dataset = set()

        for user, time in thread_posts:
            prev_posts.append((user, time))
            prev_posters.add(user)
            if user in in_dataset:
                continue
            # get ratio of posters over whole all users
            ratio += (len(prev_posters) / user_amount)
            count += 1
            in_dataset.add(user)

    return ratio / count
