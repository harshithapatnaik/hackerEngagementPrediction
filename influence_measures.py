from build_network import build_thread_info, create_user_influence_network, print_full_network
from sampling import balanced_sampling
import config
import pandas as pd
from filters import apply_filters
from features import compute_features_for_pairs

# Load config values
cfg = config.get_config_all(config)
forum_id = int(cfg["FORUM"]["ID"])
t_sus = int(cfg["TAO"]["SUSCEPTIBLE"]) * 3600
t_fos = int(cfg["TAO"]["FORGETTABLE"]) * 3600
filters_enabled = list(map(int, cfg["FILTERS"]["ENABLED"]))

# Apply filters
allowed_users, allowed_topics = apply_filters(forum_id, filters_enabled)


# Build thread info and influence network
thread_info = build_thread_info(forum_id, allowed_users, allowed_topics)
graph = create_user_influence_network(thread_info)
thread_list = list(thread_info.keys())

# Run balanced sampling
dataset = balanced_sampling(
    thread_info=thread_info,
    G=graph,
    t_sus=t_sus,
    t_fos=t_fos,
    max_pairs=500,
    output_file="outputs/balanced_samples.csv"
)

# Preview output
df = pd.read_csv("outputs/balanced_samples.csv")
print(df['label'].value_counts())
print(df.sample(min(5, len(df))))
# print_full_network(graph)

# Compute influence features and save to training set
training_df = compute_features_for_pairs(
    pairs_df=df,
    graph=graph,
    thread_info=thread_info,
    t_sus=t_sus,
    t_fos=t_fos,
    output_path="outputs/training_set.csv"
)

print("Features generated:")
print(training_df.head())
