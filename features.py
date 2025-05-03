import networkx as nx
import pandas as pd
import math
from collections import defaultdict
import config

cfg = config.get_config_all(config)


def get_influential_active_neighbors(v, thread_id, t_v, thread_info, t_sus, t_fos):
    """
    Returns a set of influential active neighbors v1 for a given user v in thread θ.
    These are users who:
    - Posted before v in thread θ within t_fos
    - v also posted after v1 in another thread within t_sus
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


def calculate_pne(G, v, infl_set, t_v, t_fos):
    """
    PNE = NAN / in-degree-at-time-t
    - infl_set: set of influential active neighbors (i.e., NAN)
    - in-degree: number of users who had an influence edge to v before time t_v
    """
    if v not in G:
        return 0

    # Compute in-degree at time t_v
    in_neighbors_at_t = set()
    for neighbor in G.predecessors(v):
        for _, edge_data in G.get_edge_data(neighbor, v).items():
            edge_time = pd.to_datetime(edge_data['date'])
            if edge_time < t_v:
                in_neighbors_at_t.add(neighbor)
                break

    if not in_neighbors_at_t:
        return 0

    pne = len(infl_set) / len(in_neighbors_at_t)
    return pne


def calculate_hub_score(infl_set, hub_set):
    """
    Counts how many of the influential neighbors belong to the global hub set.
    """
    return len(infl_set & hub_set)


def compute_features_for_pairs(df, G, thread_info, t_sus, t_fos, hub_percentile=0.1, output_path="outputs/training_set.csv"):
    """
    Computes features for each row in the input DataFrame and saves the output.
    """
    features = []

    # --- Identify global hub users ---
    out_degrees = {u: G.out_degree(u) for u in G.nodes()}
    sorted_users = sorted(out_degrees.items(), key=lambda x: x[1], reverse=True)
    cutoff = int(len(sorted_users) * hub_percentile)
    hub_set = set([u for u, _ in sorted_users[:cutoff]])

    for i, row in df.iterrows():
        v = row['user_id']
        thread_id = row['thread_id']
        t_v = pd.to_datetime(row['timestamp'])
        label = row['label']
        v1_user_id = row['v1_user_id']

        f = {'user_id': v, 'label': row['label']}

        # Reuse influential neighbors for NAN, PNE, HUB
        infl_set = get_influential_active_neighbors(v, thread_id, t_v, thread_info, t_sus, t_fos)
        if label == 0:
            infl_set.add(v1_user_id)

        if cfg['FEATURE'].get('NAN', 'False') == "True":
            f['nan'] = len(infl_set)

        if cfg['FEATURE'].get('PNE', 'False') == "True":
            f['pne'] = calculate_pne(G, v, infl_set, t_v, t_fos)

        if cfg['FEATURE'].get('HUB', 'False') == "True":
            f['hub'] = calculate_hub_score(infl_set, hub_set)

        features.append(f)

    out_df = pd.DataFrame(features)
    out_df.to_csv(output_path, index=False)
    print(f"Feature dataset written to {output_path}")
    return out_df
