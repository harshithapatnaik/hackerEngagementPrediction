import pandas as pd
import matplotlib.pyplot as plt
import os

# === Config ===
CSV_PATH = "experiment_results_forum2/feature_eval_f1_scores.csv"
OUTPUT_DIR = "experiment_results_forum2"

# Mapping from filter index to name
FILTER_NAMES = {
    "0": "posts_per_user",
    "1": "thread_per_user",
    "2": "posts_per_thread",
    "3": "users_per_thread"
}

def decode_filter_names(filter_str):
    # Convert string like "[0, 1]" → ["posts_per_user", "thread_per_user"]
    clean = filter_str.replace("[", "").replace("]", "").replace(" ", "")
    return [FILTER_NAMES.get(f, f) for f in clean.split(",") if f]

# === Load results ===
df = pd.read_csv(CSV_PATH)
df["filters"] = df["filters"].astype(str)

# === Plot each filter group ===
for filters, group in df.groupby("filters"):
    features = group["features"].tolist()
    scores = group["f1_score"].tolist()

    x = range(len(features))
    filter_names = decode_filter_names(filters)
    filter_text = "Filters used:\n" + ", ".join(filter_names)

    plt.figure(figsize=(6, 6))
    bars = plt.bar(x, scores, color="skyblue", width=0.3)

    plt.xticks(x, features, rotation=45, ha="right")
    plt.xlabel("Feature Combinations")
    plt.ylabel("F1 Score")
    plt.title(f"F1 Score by Feature Set")
    plt.ylim(0, 1.0)
    plt.grid(axis='y', linestyle='--', alpha=0.5)

    # Add score values on top of each bar
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, height + 0.02,
                 f"{height:.2f}", ha='center', va='bottom', fontsize=9)

    # # Add filter names as annotation box
    # plt.gcf().text(0.5, 0.85, filter_text, fontsize=10, va='top', ha='left',
    #                bbox=dict(boxstyle="round,pad=0.5", fc="white", ec="gray", alpha=0.5))

    plt.tight_layout()
    outname = f"f1_features_filters_{filters.replace('[','').replace(']','').replace(',','')}.png"
    plt.savefig(os.path.join(OUTPUT_DIR, outname))
    print(f"Saved: {os.path.join(OUTPUT_DIR, outname)}")
    plt.close()
