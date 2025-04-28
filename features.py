import networkx as nx
import pandas as pd
import math
from collections import defaultdict
import config

# Load feature toggles from config.yaml
cfg = config.get_config(config, "FEATURE")


# --- Feature 1: Number of Influential Active Neighbors (NAN) ---

def calculate_nan(v, thread_id, t_v, thread_info, t_sus, t_fos):
    """
    Calculates Number of Influential Active Neighbors (NAN) for a user v at time t_v in a thread.
    """
    potential_v1s = set()

    for post_id_v1, v1, t_v1_in_thread in thread_info[thread_id]:
        if v1 == v or t_v1_in_thread >= t_v:
            continue

        freshness = (t_v - t_v1_in_thread).total_seconds()
        if freshness > t_fos:
            continue  # v1 is not fresh enough

        for other_thread, posts in thread_info.items():
            if other_thread == thread_id:
                continue

            v1_post_times = [t for _, u, t in posts if u == v1]
            v_post_times = [t for _, u, t in posts if u == v]

            for t_v_other in v_post_times:
                for t_v1_other in v1_post_times:
                    delta = (t_v1_in_thread - t_v_other).total_seconds()
                    if t_v1_other < t_v_other and delta <= t_sus:
                        potential_v1s.add(v1)
                        break
                if v1 in potential_v1s:
                    break

    return len(potential_v1s)


# --- Feature 2: Personal Network Exposure (PNE) ---

def calculate_pne(df, G, t_fos):
    """
    Calculates Personal Network Exposure (PNE) for each user in the dataset.
    """
    pne_list = []

    for idx, row in df.iterrows():
        v = row['user_id']
        v_time = pd.to_datetime(row['timestamp'])

        if v not in G:
            pne_list.append(0)
            continue

        in_neighbors = list(G.predecessors(v))
        if not in_neighbors:
            pne_list.append(0)
            continue

        valid_influencers = 0
        for neighbor in in_neighbors:
            edges_data = G.get_edge_data(neighbor, v)
            if edges_data:
                for _, edge_data in edges_data.items():
                    neighbor_time = pd.to_datetime(edge_data['date'])
                    if neighbor_time <= v_time and (v_time - neighbor_time).total_seconds() <= t_fos:
                        valid_influencers += 1
                        break

        pne = valid_influencers / len(in_neighbors)
        pne_list.append(pne)

    return pne_list


# --- Feature 3: Continuous Decay ---

def compute_decay(graph, user, time):
    """
    Computes Continuous Decay of Influence for a user based on earlier influences.
    """
    decay = 0.0
    for pred in graph.predecessors(user):
        for _, edge_data in graph[pred][user].items():
            t_post = edge_data['date']
            if t_post < time:
                diff = (time - t_post).total_seconds()
                decay += math.exp(-diff / 86400)  # Decay per day
                break
    return decay


# --- Feature 4: Previous Reposts ---

def compute_previous_reposts(thread_info, user, time):
    """
    Counts how many times a user posted before time t across all threads.
    """
    count = 0
    for topic_id, posts in thread_info.items():
        for post_id, uid, t in posts:
            if uid == user and t < time:
                count += 1
    return count


# --- Feature 5: Closed Triads ---

def compute_closed_triads(graph, user):
    """
    Counts the number of closed triads formed by the user.
    """
    neighbors = set(graph.successors(user)).union(set(graph.predecessors(user)))
    count = 0
    for n1 in neighbors:
        for n2 in neighbors:
            if n1 != n2 and graph.has_edge(n1, n2):
                count += 1
    return count


# --- Feature 6: Clustering Coefficient ---

def compute_clustering(graph, user):
    """
    Computes the clustering coefficient of a user.
    """
    try:
        return nx.clustering(graph.to_undirected(), user)
    except:
        return 0.0


# --- Feature 7: Hub Users ---

def calculate_hub(df, G, hub_threshold):
    """
    Marks if a user is a Hub based on outgoing connections.
    """
    hub_list = []

    for v in df['user_id'].unique():
        if v in G:
            print(f"User: {v}, Out-Degree: {G.out_degree(v)}")
        else:
            print(f"User: {v} not found in Graph")

    for idx, row in df.iterrows():
        v = row['user_id']

        if v not in G:
            hub_list.append(0)
            continue

        out_neighbors = set(G.successors(v))
        out_degree = len(out_neighbors)

        if out_degree >= hub_threshold:
            hub_list.append(1)
        else:
            hub_list.append(0)

    return hub_list


# --- Feature 8: Mutual Reposts ---

def compute_mutual_reposts(graph, u1, u2):
    """
    Computes the number of common users both u1 and u2 influenced.
    """
    try:
        return len(set(graph.successors(u1)).intersection(set(graph.successors(u2))))
    except:
        return 0


# --- Feature 9: Active SCC Ratio ---

def compute_scc_ratio(graph, user):
    """
    Ratio of strongly connected component size containing user over total graph size.
    """
    sccs = list(nx.strongly_connected_components(graph))
    for scc in sccs:
        if user in scc:
            return len(scc) / graph.number_of_nodes()
    return 0.0


# --- Feature 10: Active SCC Count ---

def compute_scc_count(graph, user):
    """
    Number of SCCs the user is part of.
    """
    count = 0
    sccs = list(nx.strongly_connected_components(graph))
    for scc in sccs:
        if user in scc:
            count += 1
    return count


# --- MASTER Function to compute all features together ---

def compute_features_for_pairs(pairs_df, graph, thread_info, t_sus, t_fos, hub_threshold=50, output_path="outputs/training_set.csv"):
    features = []

    # Precompute batch features
    if cfg.get("PNE", "False") == "True":
        pne_values = calculate_pne(pairs_df, graph, t_fos)

    if cfg.get("HUB", "False") == "True":
        hub_values = calculate_hub(pairs_df, graph, hub_threshold)

    for idx, row in pairs_df.iterrows():
        uid = row['user_id']
        thread_id = row['thread_id']
        t = pd.to_datetime(row['timestamp'])
        label = row['label']
        v1 = row.get('v1_user_id', None)
        v1_time = pd.to_datetime(row['v1_timestamp']) if 'v1_timestamp' in row else None

        f = {'user_id': uid, 'label': label}

        if cfg.get("NAN", "False") == "True":
            f['nan'] = calculate_nan(uid, thread_id, t, thread_info, t_sus, t_fos)

        if cfg.get("PNE", "False") == "True":
            f['pne'] = pne_values[idx]

        if cfg.get("CONTINUOUS_DECAY", "False") == "True":
            f['decay'] = compute_decay(graph, uid, t)

        if cfg.get("PREVIOUS_REPOSTS", "False") == "True":
            f['prev_reposts'] = compute_previous_reposts(thread_info, uid, t)

        if cfg.get("CLOSED_TRIADS", "False") == "True":
            f['closed_triads'] = compute_closed_triads(graph, uid)

        if cfg.get("CLUSTERING", "False") == "True":
            f['clustering'] = compute_clustering(graph, uid)

        if cfg.get("HUB", "False") == "True":
            f['hub'] = hub_values[idx]

        if cfg.get("MUTUAL_REPOSTS", "False") == "True" and v1:
            f['mutual_reposts'] = compute_mutual_reposts(graph, uid, v1)

        if cfg.get("SCC_COUNT", "False") == "True":
            f['scc_count'] = compute_scc_count(graph, uid)

        if cfg.get("SCC_RATIO", "False") == "True":
            f['scc_ratio'] = compute_scc_ratio(graph, uid)

        features.append(f)

    df = pd.DataFrame(features)
    df.to_csv(output_path, index=False)
    print(f"Feature dataset written to {output_path}")
    return df
