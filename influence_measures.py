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

# Run balanced sampling — this also writes to outputs/balanced_samples.csv and outputs/imbalanced_samples.csv
_ = balanced_sampling(
    thread_info=thread_info,
    G=graph,
    t_sus=t_sus,
    t_fos=t_fos,
    max_pairs=500
)

# Load balanced sample
df_balanced = pd.read_csv("outputs/balanced_samples.csv")
print("Balanced sample stats:")
print(df_balanced['label'].value_counts())
print(df_balanced.sample(min(5, len(df_balanced))))

# Extract features for balanced set
features_balanced = compute_features_for_pairs(
    df=df_balanced,
    G=graph,
    thread_info=thread_info,
    t_sus=t_sus,
    t_fos=t_fos,
    output_path="outputs/features_on_balanced.csv"
)

# Extract features for imbalanced set
df_imbalanced = pd.read_csv("outputs/imbalanced_samples.csv")
features_imbalanced = compute_features_for_pairs(
    df=df_imbalanced,
    G=graph,
    thread_info=thread_info,
    t_sus=t_sus,
    t_fos=t_fos,
    output_path="outputs/features_on_imbalanced.csv"
)

print("Feature generation complete.")
