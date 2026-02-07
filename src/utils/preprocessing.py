# src/utils/preprocessing.py
# pour l'entraînement du modèle
"""Preprocessing functions - réutilisables pour tous les projets"""

import re
from pathlib import Path

import emoji
import nltk
import polars as pl
import unidecode
# from nltk.corpus import stopwords

# ============================================================
# SETUP STOPWORDS
# ============================================================
# try:
#     stopwords_en = set(stopwords.words("english"))
# except LookupError:
#     nltk.download("stopwords", quiet=True)
#     stopwords_en = set(stopwords.words("english"))

# ⚠️ RoBERTa n’a PAS besoin qu’on enlève les stopwords
# 👉 Tu le handicapes plus qu’autre chose.
# ============================================================
# SINGLE TEXT PREPROCESSING
# ============================================================
def preprocess_text_en(text: str, all_emojis: list) -> str:
    """
    Prétraitement adapté aux modèles Transformers (RoBERTa, BERT)
    """
    text = str(text).lower()

    # 1️⃣ Emojis → tokens
    for e in all_emojis:
        text = text.replace(
            e, f" {emoji.demojize(e, delimiters=(' emoji_', ' '))} "
        )

    # 2️⃣ Unicode normalization (é → e, etc.)
    text = unidecode.unidecode(text)

    # 3️⃣ Remove punctuation
    text = re.sub(r"[^\w\s\-]", " ", text)

    # 4️⃣ Remove extra whitespace
    text = re.sub(r"\s+", " ", text)

    # 🚫 PAS de suppression des stopwords
    return text.strip()

# def preprocess_text_en(text: str, all_emojis: list) -> str:
#     """
#     Prétraitement complet d'un texte en anglais

#     Args:
#         text: Texte à prétraiter
#         all_emojis: Liste des emojis à convertir

#     Returns:
#         Texte prétraité et nettoyé

#     Exemple:
#         >>> text = "I love this! 😍😍 Amazing!!!"
#         >>> preprocess_text_en(text, ALL_EMOJIS)
#         'love emoji heart emoji heart amazing'
#     """
#     text = str(text).lower()

#     # 1️⃣ Emojis → tokens
#     for e in all_emojis:
#         text = text.replace(e, f" {emoji.demojize(e, delimiters=(' emoji_', ' '))} ")

#     # 2️⃣ Unicode normalization (é → e, etc.)
#     text = unidecode.unidecode(text)

#     # 3️⃣ Remove punctuation
#     text = re.sub(r"[^\w\s\-]", " ", text)

#     # 4️⃣ Remove extra whitespace
#     text = re.sub(r"\s+", " ", text)

#     # 5️⃣ Remove English stopwords (the, is, a, etc.)
#     # tokens = [w for w in text.split() if w not in stopwords_en]

#     return " ".join(text).strip()


# ============================================================
# DATAFRAME PREPROCESSING
# ============================================================
def preprocess_dataframe(
    df: pl.DataFrame, text_col: str, all_emojis: list
) -> pl.DataFrame:
    """
    Applique le preprocessing à une colonne d'un DataFrame Polars

    Args:
        df: DataFrame Polars
        text_col: Nom de la colonne contenant les textes
        all_emojis: Liste des emojis à convertir

    Returns:
        DataFrame avec colonne 'text_clean' ajoutée

    Exemple:
        >>> df = pl.read_csv("data.csv")
        >>> df_clean = preprocess_dataframe(df, "text", ALL_EMOJIS)
        >>> df_clean.columns
        ['text', 'label', 'text_clean']
    """
    return df.with_columns(
        pl.col(text_col)
        .map_elements(lambda x: preprocess_text_en(x, all_emojis), return_dtype=pl.Utf8)
        .alias("text_clean")
    )



# ============================================================
# LOAD & PREPROCESS CSV
# ============================================================
def load_and_preprocess_csv(
    filepath: str, text_col: str = "text", all_emojis: list = None
) -> pl.DataFrame:
    """
    Charge et prétraite un CSV en une seule fonction

    Args:
        filepath: Chemin vers le CSV
        text_col: Nom de la colonne texte
        all_emojis: Liste des emojis

    Returns:
        DataFrame prétraité avec colonne 'text_clean'

    Exemple:
        >>> from src.utils.config import ALL_EMOJIS
        >>> df = load_and_preprocess_csv("train_data.csv", "text", ALL_EMOJIS)
    """
    filepath = Path(filepath)

    if not filepath.exists():
        raise FileNotFoundError(f"❌ Fichier non trouvé: {filepath}")

    print(f"📥 Chargement: {filepath}")
    df = pl.read_csv(filepath)

    print(f"   Dimensions: {df.shape}")
    print(f"   Colonnes: {df.columns}")

    df_clean = preprocess_dataframe(df, text_col, all_emojis)

    print("✅ Preprocessing appliqué - colonne 'text_clean' créée")

    return df_clean
