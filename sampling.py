
import random
from datetime import datetime
import csv
from tqdm import tqdm
import os

def advanced_balanced_sampling(
    thread_info,
    G,
    t_sus,
    t_fos,
    max_pairs=500,
    output_file="outputs/balanced_samples.csv"
):
    """
    Advanced balanced sampling: create up to `max_pairs` (v, v′) pairs where:
    - v is influenced by v1 (earliest user in the thread before v within tfos)
    - v has at least 2 cross-thread influenced posts from v1 (tsus + tfos)
    - v′ did not post in the same thread, but was also influenced by v1 in 2+ threads

    Stops early after `max_pairs` pairs are found.
    """
    sampled_pairs = []
    seen_posts = set()

    all_threads = list(thread_info.keys())
    random.shuffle(all_threads)

    for thread_id in tqdm(all_threads, desc="Sampling threads"):
        posts = thread_info[thread_id]
        random.shuffle(posts)

        for post_id, v, v_time in posts:
            post_key = (thread_id, v, v_time)
            if post_key in seen_posts:
                continue
            seen_posts.add(post_key)

            # Step 1: find earliest v1 in this thread before v
            v1 = None
            v1_time = None
            for post_id, u, u_time in posts:
                if u == v or u_time >= v_time:
                    continue
                if G.has_edge(u, v):
                    for _, edge_data in G[u][v].items():
                        if edge_data.get("topic") == thread_id:
                            edge_time = edge_data["date"]
                            if isinstance(edge_time, str):
                                edge_time = datetime.fromisoformat(edge_time)
                            if (v_time - edge_time).total_seconds() <= t_fos:
                                if v1 is None or edge_time < v1_time:
                                    v1 = u
                                    v1_time = edge_time

            if not v1:
                continue

            # Step 2: check v has at least 2 posts after v1 across different threads (tsus + tfos)
            v_valid = 0
            for t_id, t_posts in thread_info.items():
                t_map = {u: t for _, u, t in t_posts}
                if v1 in t_map and v in t_map:
                    delta = (t_map[v] - t_map[v1]).total_seconds()
                    if 0 < delta <= t_sus and delta <= t_fos:
                        v_valid += 1
                if v_valid >= 2:
                    break
            if v_valid < 2:
                continue

            # Step 3: find v′ (not in thread), influenced by v1 in at least 2 threads
            v_prime_candidates = set(G.successors(v1)) - set(u for _, u, _ in posts)
            for v_prime in v_prime_candidates:
                vp_valid = 0
                for t_id, t_posts in thread_info.items():
                    t_map = {u: t for _, u, t in t_posts}
                    if v1 in t_map and v_prime in t_map:
                        delta = (t_map[v_prime] - t_map[v1]).total_seconds()
                        if 0 < delta <= t_sus and delta <= t_fos:
                            vp_valid += 1
                    if vp_valid >= 2:
                        break
                if vp_valid < 2:
                    continue

                # Success: valid v, v′ pair
                sampled_pairs.append((thread_id, v, v_time, 1, v1, v1_time))
                sampled_pairs.append((thread_id, v_prime, v_time, 0, v1, v1_time))
                break  # Only one pair per v

            if len(sampled_pairs) >= 2 * max_pairs:
                break
        if len(sampled_pairs) >= 2 * max_pairs:
            break

    # Save to CSV
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["thread_id", "user_id", "timestamp", "label", "v1", "v1_timestamp"])
        writer.writerows(sampled_pairs)

    return sampled_pairs
