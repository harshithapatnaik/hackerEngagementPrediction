# import pandas as pd
# import seaborn as sns
# import matplotlib.pyplot as plt
# import os
#
# def plot_tao_f1_score():
#     RESULTS_CSV = "experiment_results/tao_eval_f1_scores.csv"
#
#     FILTER_NAME_MAP = {
#         "0": "posts_per_user",
#         "1": "thread_per_user",
#         "2": "posts_per_thread",
#         "3": "users_per_thread"
#     }
#
#     df = pd.read_csv(RESULTS_CSV)
#     df["f1_score"] = df["f1_score"].fillna(0)
#     df["filters"] = df["filters"].astype(str)
#
#     filter_sets = df["filters"].unique()
#
#     for f in filter_sets:
#         df_f = df[df["filters"] == f]
#
#         # Generate readable names for plot title
#         filter_indices = f.strip("[]").split(",")
#         filter_names = [FILTER_NAME_MAP.get(i.strip(), i.strip()) for i in filter_indices]
#         readable_name = ", ".join(filter_names)
#
#         heatmap_data = df_f.pivot(index="t_sus", columns="t_fos", values="f1_score").fillna(0)
#
#         plt.figure(figsize=(10, 6))
#         sns.heatmap(
#             heatmap_data,
#             annot=True,
#             fmt=".3f",
#             cmap="YlGnBu",
#             cbar_kws={'label': 'F1 Score'}
#         )
#         plt.title(f"F1 Score Heatmap for Filters: {readable_name}")
#         plt.xlabel("t_fos (days)")
#         plt.ylabel("t_sus (days)")
#         plt.tight_layout()
#
#         # Keep PNG filename with numbers
#         os.makedirs("experiment_results", exist_ok=True)
#         fname = f"experiment_results/tao_f1_heatmap_filters_{f.replace('[','').replace(']','').replace(',','_')}.png"
#         plt.savefig(fname)
#         plt.close()
#
#     print("Heatmap(s) saved in experiment_results/")
#
# if __name__ == "__main__":
#     plot_tao_f1_score()


import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os

def plot_tao_f1_score():
    RESULTS_CSV = "experiment_results_forum2/tao_eval_f1_scores.csv"

    # Read results
    df = pd.read_csv(RESULTS_CSV)
    df["f1_score"] = df["f1_score"].fillna(0)
    df["filters"] = df["filters"].astype(str)

    # Mapping filter IDs to names
    filter_name_map = {
        "0": "posts_per_user",
        "1": "thread_per_user",
        "2": "posts_per_thread",
        "3": "users_per_thread"
    }

    # Plot heatmap for each filter set
    for f in df["filters"].unique():
        df_f = df[df["filters"] == f]
        heatmap_data = df_f.pivot(index="t_sus", columns="t_fos", values="f1_score").fillna(0)

        plt.figure(figsize=(10, 6))
        sns.heatmap(
            heatmap_data,
            annot=True,
            fmt=".3f",
            cmap="YlGnBu",
            cbar_kws={'label': 'F1 Score'}
        )

        # Handle custom title and filename for all filters
        if f == "[0, 1, 2, 3]":
            title = "F1 Score Heatmap"
            fname = "experiment_results_forum2/tao_f1_heatmap_filters_all.png"
        else:
            readable = [filter_name_map.get(x.strip(), x.strip()) for x in f[1:-1].split(',')]
            title = f"F1 Score Heatmap ({', '.join(readable)})"
            fname = f"experiment_results_forum2/tao_f1_heatmap_filters_{f.replace('[','').replace(']','').replace(',','_')}.png"

        plt.title(title)
        plt.xlabel("t_fos (days)")
        plt.ylabel("t_sus (days)")
        plt.tight_layout()
        plt.savefig(fname)
        plt.close()

    print("Heatmap(s) saved in experiment_results_forum2/")

if __name__ == "__main__":
    plot_tao_f1_score()
