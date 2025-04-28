import pandas as pd
from build_network import build_thread_info, create_user_influence_network
import config
from features import calculate_nan, calculate_pne, calculate_hub

# This is a test file for features

# ---- Load Data ----
df = pd.read_csv("outputs/balanced_samples.csv")

# ---- Build Graph ----
cfg = config.get_config_all(config)
forum_id = int(cfg["FORUM"]["ID"])
thread_info = build_thread_info(forum_id=forum_id)
G = create_user_influence_network(thread_info)

# ---- Parameters ----
t_sus = int(cfg["TAO"]["SUSCEPTIBLE"]) * 3600  # convert hours to seconds
t_fos = int(cfg["TAO"]["FORGETTABLE"]) * 3600

# ---- Calculate NAN feature ----
nan_values = []
for idx, row in df.iterrows():
    v = row['user_id']
    thread_id = row['thread_id']
    t_v = pd.to_datetime(row['timestamp'])
    nan = calculate_nan(v, thread_id, t_v, thread_info, t_sus, t_fos)
    nan_values.append(nan)

# ---- Calculate PNE feature ----
pne_values = calculate_pne(df, G, t_fos)

# ---- Calculate HUB feature ----
hub_values = calculate_hub(df, G, hub_threshold=450)

# ---- Add features to DataFrame ----
df['NAN'] = nan_values
df['PNE'] = pne_values
df['HUB'] = hub_values

# ---- Preview Results ----
print(df[['user_id', 'label', 'NAN', 'PNE', 'HUB']])

# ---- Optional: Save Updated CSV ----
df.to_csv("outputs/balanced_samples_with_features.csv", index=False)
print("Saved updated CSV with NAN, PNE and HUB features.")
