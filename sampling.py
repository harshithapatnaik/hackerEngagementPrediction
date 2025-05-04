import random
from datetime import datetime
import csv
import os

import pandas as pd
from tqdm import tqdm

def balanced_sampling(thread_info, G, t_sus, t_fos, max_pairs=600,
                      balanced_output="outputs/balanced_samples.csv",
                      imbalanced_output="outputs/imbalanced_samples.csv"):
    """
    Balanced Sampling with separate storage of all valid negatives for imbalanced evaluation.

    Args:
        thread_info: Dictionary of thread_id → list of (post_id, user_id, timestamp)
        G: MultiDiGraph of user influence edges
        t_sus: Susceptibility window in seconds
        t_fos: Forgettability window in seconds
        max_pairs: Maximum (v, v′) pairs to generate
        balanced_output: Path to write balanced (v, v′) dataset
        imbalanced_output: Path to write imbalanced negatives for each v
    """

    balanced_data = []      # Final balanced dataset with 1 positive and 1 sampled negative per pair
    imbalanced_data = []    # Dataset containing all valid negatives per positive
    visited_post_ids = set()  # Avoid reusing the same post
    all_posts = []            # Flattened list of all posts

    # Step 0: Collect all posts into one list
    for thread_id, posts in thread_info.items():
        for post_id, user_id, timestamp in posts:
            all_posts.append((thread_id, post_id, user_id, timestamp))

    random.shuffle(all_posts)  # Randomize sampling order
    negative_counts = []
    sampled_pairs = 0  # Track total sampled pairs (each = 1 pos + 1 neg)

    # Step 1: Loop through each post to check if it's a valid (v) post
    for thread_id, post_id_v, v_user, v_post_time in tqdm(all_posts, desc="Sampling posts"):
        if post_id_v in visited_post_ids:
            continue  # Skip already used posts

        posts_in_thread = thread_info[thread_id]

        # Step 2: Find valid v1 (influential active neighbor)
        potential_v1s = []
        for pid1, u1, t1 in posts_in_thread:
            if t1 >= v_post_time or u1 == v_user:
                continue
            if (v_post_time - t1).total_seconds() <= t_fos:
                potential_v1s.append((pid1, u1, t1))

        potential_v1s.sort(key=lambda x: x[2])  # Sort by time (earliest first)

        found_valid_v1 = None

        # Step 3: Cross-thread validation of v1
        for post_id_v1, v1_user, v1_post_time in potential_v1s:
            cross_thread_valid = False
            for topic2, posts2 in thread_info.items():
                if topic2 == thread_id:
                    continue  # Must be another thread

                for post_id2, u2, t2 in posts2:
                    if u2 == v_user:
                        for post_id3, u3, t3 in posts2:
                            if u3 == v1_user and t3 < t2:
                                if 0 <= (v1_post_time - t2).total_seconds() <= t_sus:
                                    cross_thread_valid = True
                                    break
                        if cross_thread_valid:
                            break
                if cross_thread_valid:
                    break

            if cross_thread_valid:
                found_valid_v1 = (v1_user, v1_post_time, post_id_v1)
                break  # Stop at first valid v1

        if not found_valid_v1:
            continue  # Skip if no valid v1

        v1_user, v1_post_time, post_id_v1 = found_valid_v1

        # Step 4: Identify all valid v′ (negatives)
        candidates = set(G.successors(v1_user))

        # Filter out users who already posted in thread θ before v
        candidates = {
            v_prime for v_prime in candidates
            if v_prime != v_user and not any(
                t_p < v_post_time and u_p == v_prime
                for (p_id, u_p, t_p) in posts_in_thread
            )
        }

        valid_negatives = []

        for v_prime in candidates:
            for topic2, posts2 in thread_info.items():
                if topic2 == thread_id:
                    continue  # Skip thread θ

                for post_id2, u2, t2 in posts2:
                    if u2 == v_prime and pd.to_datetime(t2) < pd.to_datetime(v_post_time):
                        time_gap = (pd.to_datetime(v1_post_time) - pd.to_datetime(t2)).total_seconds()
                        if 0 <= time_gap <= t_sus:
                            valid_negatives.append((v_prime, post_id2, t2))
                            break

        if valid_negatives:
            # Step 5: Randomly choose one v′ for balanced set
            v_prime_chosen, post_id_v_prime, v_prime_post_time = random.choice(valid_negatives)

            # Add to balanced dataset: (v) and chosen (v′)
            balanced_data.append((thread_id, post_id_v, v_user, v_post_time,
                                  post_id_v1, v1_user, v1_post_time, 1))
            balanced_data.append((thread_id, post_id_v_prime, v_prime_chosen, v_prime_post_time,
                                  post_id_v1, v1_user, v1_post_time, 0))

            # Add all valid v′ to imbalanced dataset
            for v_prime, post_id_vp, t_vp in valid_negatives:
                if (v_prime, post_id_vp, t_vp) == (v_prime_chosen, post_id_v_prime, v_prime_post_time):
                    continue
                imbalanced_data.append((thread_id, post_id_vp, v_prime, t_vp,
                                        post_id_v1, v1_user, v1_post_time, 0))

            # Save negative count for this positive
            negative_counts.append((v_user, post_id_v, len(valid_negatives)))

            visited_post_ids.add(post_id_v)
            sampled_pairs += 2

        if sampled_pairs >= max_pairs:
            break

    # Step 6: Write balanced dataset to CSV
    os.makedirs(os.path.dirname(balanced_output), exist_ok=True)
    with open(balanced_output, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["thread_id", "post_id", "user_id", "timestamp",
                         "v1_post_id", "v1_user_id", "v1_timestamp", "label"])
        for row in balanced_data:
            writer.writerow(row)
    print(f"Balanced dataset written to {balanced_output}")

    # Step 7: Write imbalanced dataset to CSV
    with open(imbalanced_output, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["thread_id", "post_id", "user_id", "timestamp",
                         "v1_post_id", "v1_user_id", "v1_timestamp", "label"])
        for row in imbalanced_data:
            writer.writerow(row)
    print(f"Imbalanced dataset written to {imbalanced_output}")

    # Step 8: Save negative count per positive
    with open("outputs/negatives_per_positive.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["user_id", "post_id", "negatives_count"])
        for user_id, post_id, count in negative_counts:
            writer.writerow([user_id, post_id, count])
    print("Negative count per positive saved to outputs/negatives_per_positive.csv")

    print(f"Finished sampling {sampled_pairs} positive-negative user pairs.")
    return balanced_data
