import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os


def plot_negatives_per_positive():
    RESULTS_CSV = "experiment_results_forum2/avg_negatives_per_positive.csv"

    FILTER_NAME_MAP = {
        "0": "posts_per_user",
        "1": "threads_per_user",
        "2": "posts_per_thread",
        "3": "users_per_thread"
    }

    df = pd.read_csv(RESULTS_CSV)

    if "avg_negatives_per_positive" not in df.columns:
        print("Missing 'avg_negatives_per_positive' column.")
        return

    df["filters"] = df["filters"].astype(str)
    os.makedirs("plots_forum2", exist_ok=True)

    for f in df["filters"].unique():
        df_f = df[df["filters"] == f]
        filter_indices = f.strip("[]").split(",")
        filter_names = [FILTER_NAME_MAP.get(i.strip(), i.strip()) for i in filter_indices]
        readable_name = ", ".join(filter_names)

        heatmap_data = df_f.pivot(index="t_sus", columns="t_fos", values="avg_negatives_per_positive")
        if heatmap_data.empty:
            continue

        plt.figure(figsize=(10, 6))
        sns.heatmap(heatmap_data, annot=True, fmt=".2f", cmap="YlOrBr")
        plt.title(f"Avg Negatives per Positive")
        plt.xlabel("T_FOS (days)")
        plt.ylabel("T_SUS (days)")
        plt.tight_layout()
        plt.savefig(f"plots_forum2/negatives_per_positive_{f}.png")
        plt.close()


if __name__ == "__main__":
    plot_negatives_per_positive()
