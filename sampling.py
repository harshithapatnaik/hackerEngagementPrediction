import random
from datetime import datetime
import csv
import os
from tqdm import tqdm

def balanced_sampling(thread_info, G, t_sus, t_fos, max_pairs=500, output_file=None):
    """
    Balanced Sampling for Influence Modeling.

    For each random post (v):
      1. Find earliest v1 who posted before v in the same thread within t_fos (forgettability window).
      2. Check cross-thread that v posted AFTER v1 at least once, and v1's post in thread θ is within t_sus.
      3. If valid v1 is found, search for v′ influenced by v1 (same logic, but v′ shouldn't have posted in thread θ before v).
      4. Sample (v, v′) pair as (positive, negative).

    Args:
        thread_info: Dictionary of thread_id → list of (post_id, user_id, timestamp)
        G: MultiDiGraph of user influence edges
        t_sus: Susceptibility window in seconds
        t_fos: Forgettability window in seconds
        max_pairs: Maximum (v, v′) pairs to generate
        output_file: Path to CSV to save dataset

    Returns:
        balanced_data: List of (thread_id, post_id, user_id, timestamp, v1_post_id, v1_user_id, v1_timestamp, label)
    """

    balanced_data = []  # Output: List of samples
    visited_post_ids = set()  # To avoid sampling the same post again
    all_posts = []  # Flattened list of all posts

    # Step 0: Prepare posts list [(thread_id, post_id, user_id, post_time)]
    for thread_id, posts in thread_info.items():
        for post_id, user_id, timestamp in posts:
            all_posts.append((thread_id, post_id, user_id, timestamp))

    random.shuffle(all_posts)  # Randomize order

    sampled_pairs = 0

    for thread_id, post_id_v, v_user, v_post_time in tqdm(all_posts, desc="Sampling posts"):

        # Skip if post already sampled
        if post_id_v in visited_post_ids:
            continue

        posts_in_thread = thread_info[thread_id]

        # Step 1: Find influential active neighbor v1
        potential_v1s = []
        for pid1, u1, t1 in posts_in_thread:
            if t1 >= v_post_time or u1 == v_user:
                continue  # Skip posts after v or from v itself
            forgettability_gap = (v_post_time - t1).total_seconds()
            if forgettability_gap <= t_fos:
                potential_v1s.append((pid1, u1, t1))

        potential_v1s.sort(key=lambda x: x[2])  # Earliest first

        found_valid_v1 = None

        for post_id_v1, v1_user, v1_post_time in potential_v1s:
            cross_thread_valid = False

            # Cross-thread susceptibility check
            for topic2, posts2 in thread_info.items():
                if topic2 == thread_id:
                    continue  # Skip same thread

                for post_id2, u2, t2 in posts2:
                    if u2 == v_user:
                        # v posted in another thread
                        for post_id3, u3, t3 in posts2:
                            if u3 == v1_user and t3 < t2:
                                # v1 posted before v in other thread
                                time_diff = (v1_post_time - t2).total_seconds()
                                if 0 <= time_diff <= t_sus:
                                    cross_thread_valid = True
                                    break
                        if cross_thread_valid:
                            break
                if cross_thread_valid:
                    break

            if cross_thread_valid:
                found_valid_v1 = (v1_user, v1_post_time, post_id_v1)
                break  # Stop after finding first valid v1

        if not found_valid_v1:
            continue  # No valid v1 found, skip

        v1_user, v1_post_time, post_id_v1 = found_valid_v1

        # Step 2: Find negative user v′ under v1
        candidates = set(G.successors(v1_user))

        # Filter: User shouldn't have posted in this thread before v
        candidates = {
            v_prime for v_prime in candidates
            if not any(
                t_p < v_post_time and u_p == v_prime
                for (p_id, u_p, t_p) in posts_in_thread
            )
        }

        valid_negatives = []

        for v_prime in candidates:
            for topic2, posts2 in thread_info.items():
                if topic2 == thread_id:
                    continue  # Skip θ

                for post_id2, u2, t2 in posts2:
                    if u2 == v_prime:
                        time_gap = (v1_post_time - t2).total_seconds()
                        if 0 <= time_gap <= t_sus:
                            valid_negatives.append((v_prime, post_id2, t2))
                            break
                if v_prime in [v for v, _, _ in valid_negatives]:
                    break

        if valid_negatives:
            v_prime, post_id_v_prime, v_prime_post_time = random.choice(valid_negatives)

            # Save both positive and negative examples
            balanced_data.append((thread_id, post_id_v, v_user, v_post_time, post_id_v1, v1_user, v1_post_time, 1))
            balanced_data.append((thread_id, post_id_v_prime, v_prime, v_prime_post_time, post_id_v1, v1_user, v1_post_time, 0))

            visited_post_ids.add(post_id_v)
            sampled_pairs += 1

        if sampled_pairs >= max_pairs:
            break

    # Step 3: Save to CSV
    if output_file:
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["thread_id", "post_id", "user_id", "timestamp", "v1_post_id", "v1_user_id", "v1_timestamp", "label"])
            for row in balanced_data:
                writer.writerow(row)
        print(f"Balanced dataset written to {output_file}")

    print(f"Finished sampling {sampled_pairs} positive-negative user pairs.")
    return balanced_data
