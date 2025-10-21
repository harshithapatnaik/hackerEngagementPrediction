import pandas as pd
import matplotlib.pyplot as plt
from sklearn.ensemble import ExtraTreesClassifier, RandomForestClassifier, AdaBoostClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.svm import SVC
from sklearn.neighbors import KNeighborsClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.metrics import f1_score, precision_score, recall_score
from sklearn.model_selection import train_test_split
from sklearn.utils import shuffle
from xgboost import XGBClassifier

import config
import os

# Load the enabled features from config.yaml
cfg = config.get_config_all(config)
feature_list = [f.lower() for f, enabled in cfg['FEATURE'].items() if enabled == "True"]

# Load feature files
balanced_df = pd.read_csv("outputs/features_on_balanced.csv")
imbalanced_df = pd.read_csv("outputs/features_on_imbalanced.csv")

# Split the balanced dataset: 80% for training, 20% + all non-adoptors for testing
train_df, test_df_balanced = train_test_split(
    balanced_df, test_size=0.2, stratify=balanced_df['label'], random_state=42
)

test_df = pd.concat([
        test_df_balanced,
        imbalanced_df[imbalanced_df['label'] == 0]
    ], ignore_index=True)

# Shuffle the final test set
test_df = shuffle(test_df, random_state=42)

# Prepare training and testing input/output
X_train = train_df[feature_list]
y_train = train_df['label']
X_test = test_df[feature_list]
y_test = test_df['label']

# Set up the models to evaluate
models = {
    'RF': RandomForestClassifier(random_state=42),
    'ADA': AdaBoostClassifier(random_state=42),
    'SVC': SVC(probability=True, random_state=42),
    'KNN': KNeighborsClassifier(n_neighbors=5),
    'NB': GaussianNB(),
    'XGB': XGBClassifier(use_label_encoder=False, eval_metric='logloss', random_state=42),
    'MLP': MLPClassifier(hidden_layer_sizes=(64,), solver='adam', max_iter=500, random_state=42)
}

# Run each model and store the metrics
f1_scores = []
recall_scores = []
precision_scores = []
labels = []

for name, model in models.items():
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    f1 = f1_score(y_test, y_pred)
    rec = recall_score(y_test, y_pred)
    prec = precision_score(y_test, y_pred)

    f1_scores.append(f1 * 100)
    recall_scores.append(rec * 100)
    precision_scores.append(prec * 100)
    labels.append(name)

# Plot the results
x = range(len(labels))
bar_width = 0.25

plt.figure(figsize=(12, 6))
plt.bar([i - bar_width for i in x], f1_scores, width=bar_width, label='F1', color='skyblue')
plt.bar(x, recall_scores, width=bar_width, label='Recall', color='cornflowerblue')
plt.bar([i + bar_width for i in x], precision_scores, width=bar_width, label='Precision', color='blue')

plt.xlabel('Classification Algorithm')
plt.ylabel('Score')
plt.title('Model Performance (Train: 80% Balanced | Test: 20% Balanced + All Negatives)')
plt.xticks(x, labels)
plt.legend()
plt.tight_layout()
plt.grid(axis='y', linestyle='--', linewidth=0.5)
os.makedirs("outputs", exist_ok=True)
plt.savefig("outputs/model_comparison_balanced_train_imbalanced_test.png")
plt.show()
