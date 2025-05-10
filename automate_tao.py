import os
import itertools
import pandas as pd
from sklearn.svm import SVC
from sklearn.metrics import f1_score
from sklearn.model_selection import train_test_split
from sklearn.utils import shuffle

from filters import apply_filters
from build_network import build_thread_info, create_user_influence_network
from sampling import balanced_sampling
from features import compute_features_for_pairs

# === Config ===
FILTER_SETS = [[0, 1, 2, 3]]
TAO_DAYS = [7, 14, 21, 28, 60, 90, 180, 365]
MAX_PAIRS = 500
FEATURES_USED = ["nan", "pne", "hub"]
RESULTS_CSV = "experiment_results_forum2/tao_eval_f1_scores.csv"
NEG_STATS_CSV = "experiment_results_forum2/avg_negatives_per_positive.csv"
FORUM_ID = 2

# Create result directories
os.makedirs("experiment_results_forum2", exist_ok=True)
os.makedirs("outputs", exist_ok=True)

# === Helper: Run SVC and compute F1 score ===
def run_svc_model(feature_cols):
    try:
        balanced_df = pd.read_csv("outputs/features_on_balanced.csv")
        imbalanced_df = pd.read_csv("outputs/features_on_imbalanced.csv")

        train_df, test_df_balanced = train_test_split(
            balanced_df, test_size=0.2, stratify=balanced_df['label'], random_state=42
        )

        test_df = pd.concat([
            test_df_balanced,
            imbalanced_df[imbalanced_df['label'] == 0]
        ], ignore_index=True)

        test_df = shuffle(test_df, random_state=42)

        X_train = train_df[feature_cols]
        y_train = train_df['label']
        X_test = test_df[feature_cols]
        y_test = test_df['label']

        model = SVC(probability=True, random_state=42)
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)

        return round(f1_score(y_test, y_pred), 4)

    except Exception as e:
        print(f"[Model Error] {e}")
        return None


# === Run TAO Evaluation Loop ===
if __name__ == "__main__":
    results = []
    neg_stats = []

    for filters in FILTER_SETS:
        print(f"\n=== [FILTERS: {filters}] Initializing Graph and Threads ===")
        allowed_users, allowed_topics = apply_filters(forum_id=FORUM_ID, active_filters=filters)
        thread_info = build_thread_info(forum_id=FORUM_ID, allowed_users=allowed_users, allowed_topics=allowed_topics)

        if not thread_info:
            print("No valid threads found. Skipping filter:", filters)
            continue

        G = create_user_influence_network(thread_info)

        for t_sus_days, t_fos_days in itertools.product(TAO_DAYS, TAO_DAYS):
            t_sus = t_sus_days * 24 * 3600  # convert to seconds
            t_fos = t_fos_days * 24 * 3600

            print(f"\n→ [Filters: {filters}] TAO: t_sus={t_sus_days}d, t_fos={t_fos_days}d")

            # Run sampling
            balanced_sampling(
                thread_info=thread_info,
                G=G,
                t_sus=t_sus,
                t_fos=t_fos,
                max_pairs=MAX_PAIRS
            )

            # Load sampled data
            df_balanced = pd.read_csv("outputs/balanced_samples.csv")
            df_imbalanced = pd.read_csv("outputs/imbalanced_samples.csv")

            # Compute features
            compute_features_for_pairs(
                df=df_balanced,
                G=G,
                thread_info=thread_info,
                t_sus=t_sus,
                t_fos=t_fos,
                output_path="outputs/features_on_balanced.csv"
            )

            compute_features_for_pairs(
                df=df_imbalanced,
                G=G,
                thread_info=thread_info,
                t_sus=t_sus,
                t_fos=t_fos,
                output_path="outputs/features_on_imbalanced.csv"
            )

            # Compute average negatives per positive
            try:
                neg_counts = pd.read_csv("outputs/negatives_per_positive.csv")
                avg_neg_per_pos = round(neg_counts['negatives_count'].mean(), 2)
            except Exception as e:
                print(f"[NegStat Warning] Could not compute negatives per positive: {e}")
                avg_neg_per_pos = 0.0

            # Evaluate model
            f1 = run_svc_model(FEATURES_USED)

            results.append({
                "filters": str(filters),
                "t_sus": t_sus_days,
                "t_fos": t_fos_days,
                "f1_score": f1
            })

            neg_stats.append({
                "filters": str(filters),
                "t_sus": t_sus_days,
                "t_fos": t_fos_days,
                "avg_negatives_per_positive": avg_neg_per_pos
            })

    # Save results
    pd.DataFrame(results).to_csv(RESULTS_CSV, index=False)
    pd.DataFrame(neg_stats).to_csv(NEG_STATS_CSV, index=False)

    print(f"\nAll TAO combination results saved to: {RESULTS_CSV}")
    print(f"Negatives per positive averages saved to: {NEG_STATS_CSV}")
