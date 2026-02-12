import polars as pl
from src.utils import (
    ALL_EMOJIS,
    DATA_PATH,
    SEED,
    load_and_preprocess_csv,
    preprocess_dataframe,
)

TARGET_PER_CLASS = 6000
SPLITS = {'train': 'train_df.csv', 'validation': 'val_df.csv', 'test': 'test_df.csv'}

# -------------------------------
# Chargement et préparation Sp1786
# -------------------------------
df_Sp1786 = pl.read_csv(
    'hf://datasets/Sp1786/multiclass-sentiment-analysis-dataset/' + SPLITS['train']
)
df_Sp1786 = df_Sp1786.filter(pl.col("sentiment") == "neutral")
df_Sp1786 = df_Sp1786.rename({"label": "label_id", "sentiment": "label"})
df_Sp1786 = preprocess_dataframe(df_Sp1786, text_col="text", all_emojis=ALL_EMOJIS)
df_Sp1786 = df_Sp1786.select(["text_clean", "label"])

# -------------------------------
# Chargement et préparation sentimentdataset
# -------------------------------
df_neutral = load_and_preprocess_csv(
    str(DATA_PATH / "version2_7classes_en" / "sentimentdataset.csv"),
    text_col="Text",
    all_emojis=ALL_EMOJIS
)
df_neutral = df_neutral.rename({"Sentiment": "label"})
df_neutral = df_neutral.select(["text_clean", "label"])
df_neutral = df_neutral.with_columns(pl.col("label").str.strip_chars().str.to_lowercase())
df_neutral = df_neutral.filter(pl.col("label").is_in(["neutral", "fear", "love", "surprise"]))

# -------------------------------
# Chargement et préparation text.csv
# -------------------------------
df_emotion = load_and_preprocess_csv(
    str(DATA_PATH / "version2_7classes_en" / "text.csv"),
    text_col="text",
    all_emojis=ALL_EMOJIS
)
# garder uniquement text_clean et label
df_emotion = df_emotion.select(["text_clean", "label"])

label_map = {
    0: "sad",
    1: "joy",
    2: "love",
    3: "anger",
    4: "fear",
    5: "surprise",
}

df_emotion = df_emotion.with_columns(
    pl.col("label").cast(pl.Utf8).replace(label_map).alias("label")
)

# -------------------------------
# Concaténation des datasets
# -------------------------------
df_raw = pl.concat([df_Sp1786, df_neutral, df_emotion])

# -------------------------------
# Équilibrage des classes (undersampling)
# -------------------------------
dfs = []
for label in sorted(df_raw["label"].unique()):
    df_l = df_raw.filter(pl.col("label") == label)
    n_rows = len(df_l)
    if n_rows >= TARGET_PER_CLASS:
        df_l = df_l.sample(n=TARGET_PER_CLASS, seed=SEED)
        print(f"{label:10s}: {TARGET_PER_CLASS} (undersample)")
    else:
        print(f"{label:10s}: {n_rows} (keep all)")
    dfs.append(df_l)

df_balanced = pl.concat(dfs).sample(fraction=1.0, seed=SEED)

print("\n📊 Distribution équilibrée:")
print(df_balanced.group_by("label").agg(pl.count()))

# -------------------------------
# Sauvegarde finale
# -------------------------------
df_balanced.write_csv(DATA_PATH / "version2_7classes_en" / "text_7_labels_en.csv")
print("\n✅ Dataset sauvegardé sous 'text_7_labels_en.csv'")
