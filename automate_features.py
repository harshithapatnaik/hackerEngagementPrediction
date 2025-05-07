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

# === Global Config ===
ALL_FEATURES = ["NAN", "PNE", "HUB"]         # All possible influence features
FILTER_SETS = [[0, 1, 2, 3]]               # Two filter sets to test
TAO_SUS = 8760 * 3600                        # 1 year (in seconds) for susceptibility window
TAO_FOS = 8760 * 3600                        # 1 year (in seconds) for forgettability window
MAX_PAIRS = 500                              # Limit on (v, v') pairs sampled per run
FORUM_ID = 2

RESULTS_CSV = "experiment_results_forum2/feature_eval_f1_scores.csv"
os.makedirs("experiment_results_forum2", exist_ok=True)
os.makedirs("outputs", exist_ok=True)

# === Helper: Generate all non-empty subsets of features ===
def get_feature_subsets(features):
    combos = []
    for i in range(1, len(features) + 1):
        combos.extend(itertools.combinations(features, i))
    return combos

# === Helper: Train SVC and compute F1 on balanced + imbalanced test set ===
def run_svc_model(feature_cols):
    try:
        # Load previously extracted features
        balanced_df = pd.read_csv("outputs/features_on_balanced.csv")
        imbalanced_df = pd.read_csv("outputs/features_on_imbalanced.csv")

        # Train/test split on balanced data (80% train, 20% test)
        train_df, test_df_balanced = train_test_split(
            balanced_df, test_size=0.2, stratify=balanced_df['label'], random_state=42
        )

        test_df = pd.concat([
            test_df_balanced,
            imbalanced_df[imbalanced_df['label'] == 0]
        ], ignore_index=True)
        test_df = shuffle(test_df, random_state=42)

        # Train SVC on selected features
        X_train = train_df[feature_cols]
        y_train = train_df['label']
        X_test = test_df[feature_cols]
        y_test = test_df['label']

        model = SVC(probability=True, random_state=42)
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)

        # Return rounded F1 score
        return round(f1_score(y_test, y_pred), 4)

    except Exception as e:
        print(f"[Model Error] {e}")
        return None

# === Main Evaluation Loop ===
if __name__ == "__main__":
    results = []
    feature_combos = get_feature_subsets(ALL_FEATURES)

    for filters in FILTER_SETS:
        print(f"\n=== [FILTERS: {filters}] Running network + sampling once ===")

        # STEP 1: Apply filters to limit users/threads
        allowed_users, allowed_topics = apply_filters(forum_id=FORUM_ID, active_filters=filters)

        # STEP 2: Build thread info from filtered data
        thread_info = build_thread_info(forum_id=FORUM_ID, allowed_users=allowed_users, allowed_topics=allowed_topics)
        if not thread_info:
            print("No threads after filter. Skipping.")
            continue

        # STEP 3: Build influence graph (who influenced whom)
        G = create_user_influence_network(thread_info)

        # STEP 4: Sample balanced and imbalanced (v, v′) user pairs
        balanced_sampling(
            thread_info=thread_info,
            G=G,
            t_sus=TAO_SUS,
            t_fos=TAO_FOS,
            max_pairs=MAX_PAIRS
        )

        # STEP 5: Load sampled datasets once
        df_balanced = pd.read_csv("outputs/balanced_samples.csv")
        df_imbalanced = pd.read_csv("outputs/imbalanced_samples.csv")

        # STEP 6: Loop through each feature subset (no need to resample)
        for feature_set in feature_combos:
            feature_cols = [f.lower() for f in feature_set]
            feature_name = "+".join(feature_set)
            print(f"\n→ [Evaluating Features: {feature_set}]")

            # Extract only these features into new CSVs
            compute_features_for_pairs(
                df=df_balanced,
                G=G,
                thread_info=thread_info,
                t_sus=TAO_SUS,
                t_fos=TAO_FOS,
                output_path="outputs/features_on_balanced.csv"
            )
            compute_features_for_pairs(
                df=df_imbalanced,
                G=G,
                thread_info=thread_info,
                t_sus=TAO_SUS,
                t_fos=TAO_FOS,
                output_path="outputs/features_on_imbalanced.csv"
            )

            # Run SVC on the selected features
            f1 = run_svc_model(feature_cols)

            # Save result row
            results.append({
                "filters": str(filters),
                "features": feature_name,
                "f1_score": f1
            })

    # Final CSV with all scores
    pd.DataFrame(results).to_csv(RESULTS_CSV, index=False)
    print(f"\nAll feature combination results saved to: {RESULTS_CSV}")


#
# import os
# import itertools
# import pandas as pd
# from sklearn.svm import SVC
# from sklearn.metrics import f1_score
# from sklearn.model_selection import train_test_split
# from sklearn.utils import shuffle
#
# from filters import apply_filters
# from build_network import build_thread_info, create_user_influence_network
# from sampling import balanced_sampling
# from features import compute_features_for_pairs
#
# # === Configuration ===
# ALL_FEATURES = ["NAN", "PNE", "HUB"]
# FILTER_SETS = [[0, 1], [2, 3]]
# TAO_SUS = 8760 * 3600  # in seconds
# TAO_FOS = 8760 * 3600  # in seconds
# MAX_PAIRS = 500
#
# # Output CSV for results
# RESULTS_CSV = "experiment_results/feature_eval_f1_scores.csv"
# os.makedirs("experiment_results", exist_ok=True)
#
# # === Helper to generate feature subsets ===
# def get_feature_subsets(features):
#     combos = []
#     for i in range(1, len(features) + 1):
#         combos.extend(itertools.combinations(features, i))
#     return combos
#
# # === Main Evaluation Pipeline ===
# def run_pipeline(filters_enabled, feature_subset):
#     print(f"\n[RUNNING] Filters: {filters_enabled}, Features: {feature_subset}")
#
#     # 1. Apply filters
#     allowed_users, allowed_topics = apply_filters(forum_id=8, active_filters=filters_enabled)
#
#     # 2. Build threads and graph
#     thread_info = build_thread_info(forum_id=8, allowed_users=allowed_users, allowed_topics=allowed_topics)
#     if not thread_info:
#         print("No thread info found. Skipping.")
#         return None
#
#     G = create_user_influence_network(thread_info)
#
#     # 3. Run balanced sampling
#     balanced_sampling(
#         thread_info=thread_info,
#         G=G,
#         t_sus=TAO_SUS,
#         t_fos=TAO_FOS,
#         max_pairs=MAX_PAIRS
#     )
#
#     # 4. Extract features
#     feature_flags = {f: "True" if f in feature_subset else "False" for f in ALL_FEATURES}
#
#     df_balanced = pd.read_csv("outputs/balanced_samples.csv")
#     compute_features_for_pairs(
#         df=df_balanced,
#         G=G,
#         thread_info=thread_info,
#         t_sus=TAO_SUS,
#         t_fos=TAO_FOS,
#         output_path="outputs/features_on_balanced.csv"
#     )
#
#     df_imbalanced = pd.read_csv("outputs/imbalanced_samples.csv")
#     compute_features_for_pairs(
#         df=df_imbalanced,
#         G=G,
#         thread_info=thread_info,
#         t_sus=TAO_SUS,
#         t_fos=TAO_FOS,
#         output_path="outputs/features_on_imbalanced.csv"
#     )
#
#     # 5. Train + Evaluate SVC using selected features
#     feature_cols = [f.lower() for f in feature_subset]
#
#     try:
#         balanced_df = pd.read_csv("outputs/features_on_balanced.csv")
#         imbalanced_df = pd.read_csv("outputs/features_on_imbalanced.csv")
#         train_df, test_df_balanced = train_test_split(
#             balanced_df, test_size=0.2, stratify=balanced_df['label'], random_state=42
#         )
#         num_pos = test_df_balanced[test_df_balanced['label'] == 1].shape[0]
#         test_neg = imbalanced_df[imbalanced_df['label'] == 0].sample(n=51 * num_pos, random_state=42)
#         test_df = pd.concat([test_df_balanced[test_df_balanced['label'] == 1], test_neg], ignore_index=True)
#         test_df = shuffle(test_df, random_state=42)
#
#         X_train = train_df[feature_cols]
#         y_train = train_df['label']
#         X_test = test_df[feature_cols]
#         y_test = test_df['label']
#
#         model = SVC(probability=True, random_state=42)
#         model.fit(X_train, y_train)
#         y_pred = model.predict(X_test)
#         f1 = f1_score(y_test, y_pred)
#
#         # 6. Save result
#         return {
#             "filters": str(filters_enabled),
#             "features": "+".join(feature_subset),
#             "f1_score": round(f1, 4)
#         }
#
#     except Exception as e:
#         print(f"[Error during model evaluation] {e}")
#         return None
#
#
# # === Run All Experiments ===
# if __name__ == "__main__":
#     results = []
#     feature_combos = get_feature_subsets(ALL_FEATURES)
#
#     for filters in FILTER_SETS:
#         for feature_set in feature_combos:
#             result = run_pipeline(filters, list(feature_set))
#             if result:
#                 results.append(result)
#
#     # Save results to CSV
#     pd.DataFrame(results).to_csv(RESULTS_CSV, index=False)
#     print(f"\n✅ All results saved to: {RESULTS_CSV}")

