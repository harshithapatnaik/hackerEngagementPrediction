from itertools import combinations
import networkx as nx
import pandas as pd
from collections import defaultdict
import config

cfg = config.get_config_all(config)


def get_influential_active_neighbors(v, thread_id, t_v, thread_info, t_sus, t_fos):
    """
    Returns IANs of user v in thread θ (before t_v), satisfying:
    - forgettability (within t_fos)
    - activity (v posted after v1 in another thread within t_sus)
    """
    potential_v1s = set()

    for post_id_v1, v1, t_v1_in_thread in thread_info[thread_id]:
        if v1 == v or pd.to_datetime(t_v1_in_thread) >= pd.to_datetime(t_v):
            continue

        freshness = (pd.to_datetime(t_v) - pd.to_datetime(t_v1_in_thread)).total_seconds()
        if freshness > t_fos:
            continue

        for other_thread, posts in thread_info.items():
            if other_thread == thread_id:
                continue

            v1_post_times = [t for _, u, t in posts if u == v1]
            v_post_times = [t for _, u, t in posts if u == v]

            for t_v_other in v_post_times:
                for t_v1_other in v1_post_times:
                    delta = (pd.to_datetime(t_v1_in_thread) - pd.to_datetime(t_v_other)).total_seconds()
                    if pd.to_datetime(t_v1_other) < pd.to_datetime(t_v_other) and delta <= t_sus:
                        potential_v1s.add(v1)
                        break
                if v1 in potential_v1s:
                    break

    return potential_v1s


def get_all_influential_active_neighbors(v, t_v, thread_info, t_sus, t_fos):
    """
    Returns IANs of user v across all threads (including current thread), before t_v.
    Same as get_influential_active_neighbors but global across all threads.
    """
    t_v = pd.to_datetime(t_v)
    all_v1s = set()

    for thread_id, posts in thread_info.items():
        for post_id_v1, v1, t_v1_in_thread in posts:
            t_v1_in_thread = pd.to_datetime(t_v1_in_thread)

            if v1 == v or t_v1_in_thread >= t_v:
                continue

            freshness = (t_v - t_v1_in_thread).total_seconds()
            if freshness > t_fos:
                continue

            for other_thread, other_posts in thread_info.items():
                if other_thread == thread_id:
                    continue

                v1_post_times = [pd.to_datetime(t) for _, u, t in other_posts if u == v1]
                v_post_times = [pd.to_datetime(t) for _, u, t in other_posts if u == v]

                for t_v_other in v_post_times:
                    for t_v1_other in v1_post_times:
                        delta = (t_v1_in_thread - t_v_other).total_seconds()
                        if t_v1_other < t_v_other and delta <= t_sus:
                            all_v1s.add(v1)
                            break
                    if v1 in all_v1s:
                        break

    return all_v1s


def calculate_pne(infl_set, all_ian_set):
    """
    PNE = |IANs in current thread| / |IANs across all threads|
    """
    if not all_ian_set:
        return 0
    return len(infl_set) / len(all_ian_set)


def calculate_hub_score(infl_set, hub_set):
    """
    HUB = count of IANs that are also in the global hub set
    """
    return len(infl_set & hub_set)


def has_edge_before_t_v(G, u, z, t_v):
    """
    True if there exists an edge u→z or z→u before t_v
    """
    if G.has_edge(u, z):
        for data in G.get_edge_data(u, z).values():
            if 'date' in data and pd.to_datetime(data['date']) <= t_v:
                return True
    if G.has_edge(z, u):
        for data in G.get_edge_data(z, u).values():
            if 'date' in data and pd.to_datetime(data['date']) <= t_v:
                return True
    return False


def calculate_open_triads(G, all_ian_set, t_v):
    """
    OPT = # of unordered IAN pairs (u, z) where u↔z edge exists before t_v
    """
    infl_nodes = [u for u in all_ian_set]
    if len(infl_nodes) < 2:
        return 0

    opt_count = 0
    for u, z in combinations(infl_nodes, 2):
        if has_edge_before_t_v(G, u, z, t_v):
            opt_count += 1
    return opt_count


def get_total_possible_triads_for_v(G, v, t_v, thread_info, t_sus, t_fos):
    """
    Counts how many unordered pairs (u, z) of IANs of v are connected before t_v
    – i.e., total possible triads v could be part of.
    """
    infl_set = get_all_influential_active_neighbors(v, t_v, thread_info, t_sus, t_fos)
    infl_nodes = [u for u in infl_set if u != v]

    if len(infl_nodes) < 2:
        return 0

    triangle_count = 0
    for u, z in combinations(infl_nodes, 2):
        if has_edge_before_t_v(G, u, z, t_v):
            triangle_count += 1

    return triangle_count



def calculate_clustering_coefficient(open_triads, total_possible_triads):
    """
    CLC = OPT / total_possible_triads
    """
    if total_possible_triads == 0:
        return 0.0
    return open_triads / total_possible_triads


def compute_features_for_pairs(df, G, thread_info, t_sus, t_fos, hub_percentile=0.1,
                               output_path="outputs/training_set.csv"):
    """
    Main function to compute NAN, PNE, HUB, OPT, CLC
    for each (v, v') pair in the dataframe.
    """
    features = []

    # --- Compute global hub set ---
    out_degrees = {u: G.out_degree(u) for u in G.nodes()}
    sorted_users = sorted(out_degrees.items(), key=lambda x: x[1], reverse=True)
    cutoff = int(len(sorted_users) * hub_percentile)
    hub_set = set([u for u, _ in sorted_users[:cutoff]])

    for _, row in df.iterrows():
        v = row['user_id']
        thread_id = row['thread_id']
        t_v = pd.to_datetime(row['timestamp'])
        label = row['label']
        v1_user_id = row['v1_user_id']

        f = {'user_id': v, 'label': label}

        # Precompute IANs in thread and across all threads
        infl_set = get_influential_active_neighbors(v, thread_id, t_v, thread_info, t_sus, t_fos)
        all_ian_set = get_all_influential_active_neighbors(v, t_v, thread_info, t_sus, t_fos)

        if label == 0:
            infl_set.add(v1_user_id)
            all_ian_set.add(v1_user_id)

        # NAN
        if cfg['FEATURE'].get('NAN', 'False') == "True":
            f['nan'] = len(infl_set)

        # PNE
        if cfg['FEATURE'].get('PNE', 'False') == "True":
            f['pne'] = calculate_pne(infl_set, all_ian_set)

        # HUB
        if cfg['FEATURE'].get('HUB', 'False') == "True":
            f['hub'] = calculate_hub_score(infl_set, hub_set)

        # OPT and CLC
        if cfg['FEATURE'].get('OPT', 'False') == "True" or cfg['FEATURE'].get('CLC', 'False') == "True":
            opt_count = calculate_open_triads(G, all_ian_set, t_v)

            if cfg['FEATURE'].get('OPT', 'False') == "True":
                f['opt'] = opt_count

            if cfg['FEATURE'].get('CLC', 'False') == "True":
                total_triads = get_total_possible_triads_for_v(G, v, t_v, thread_info, t_sus, t_fos)
                f['clc'] = calculate_clustering_coefficient(opt_count, total_triads)

        features.append(f)

    out_df = pd.DataFrame(features)
    out_df.to_csv(output_path, index=False)
    print(f"Feature dataset written to {output_path}")
    return out_df
