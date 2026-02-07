import re
import unicodedata

import polars as pl
from beartype import beartype
from langdetect import LangDetectException, detect

# -----------------------
# TEXT EXTRACTION
# -----------------------


@beartype
def extract_hashtags(text: str) -> list[str]:
    """Extrait tous les hashtags (toutes langues)."""
    return re.findall(r"#\S+", text)


@beartype
def extract_emojis(text: str) -> list[str]:
    """Extrait tous les emojis."""
    return [char for char in text if unicodedata.category(char).startswith("So")]


# -----------------------
# TEXT CLEANING
# -----------------------


@beartype
def clean_text(text: str) -> str:
    """
    Nettoie le texte principal :
    - enlève URLs, mentions
    - enlève hashtags (déjà stockés ailleurs)
    - enlève emojis (déjà stockés ailleurs)
    - remplace plusieurs espaces par un seul
    - supprime les espaces en début/fin
    - lowercase
    """
    # enlever URLs et mentions
    text = re.sub(r"https?://\S+", "", text)
    text = re.sub(r"@\w+", "", text)

    # enlever hashtags
    text = re.sub(r"#\S+", "", text)

    # enlever emojis
    text = "".join(
        char for char in text if not unicodedata.category(char).startswith("So")
    )

    # remplacer plusieurs espaces par un seul
    text = re.sub(r"\s+", " ", text)

    # enlever espaces début/fin
    return text.strip().lower()


# -----------------------
# QUALITY CHECKS
# -----------------------


@beartype
def detect_short_spam(df: pl.DataFrame, min_tokens: int = 3) -> pl.DataFrame:
    """Ajoute token_count, is_short, is_spam."""
    return df.with_columns(
        [
            pl.col("clean_content")
            .map_elements(lambda x: len(x.split()))
            .alias("token_count"),
            pl.col("clean_content")
            .map_elements(lambda x: len(x.split()) < min_tokens)
            .alias("is_short"),
            pl.col("clean_content")
            .map_elements(lambda x: bool(re.match(r"^[^\w\s]{0,3}$", x)))
            .alias("is_spam"),
        ]
    )


@beartype
def detect_non_latin(df: pl.DataFrame) -> pl.DataFrame:
    """
    True si présence d'un alphabet non latin
    (japonais, arabe, cyrillique, etc.)
    """

    def check_latin(text: str) -> bool:
        for char in text:
            if char.isalpha():
                try:
                    name = unicodedata.name(char)
                except ValueError:
                    continue
                if "LATIN" not in name:
                    return True
        return False

    return df.with_columns(
        pl.col("clean_content").map_elements(check_latin).alias("is_non_latin")
    )


# -----------------------
# LANGUAGE DETECTION
# -----------------------


@beartype
def detect_language(text: str) -> str | None:
    try:
        return detect(text)
    except LangDetectException:
        return None


# -----------------------
# CONFIDENCE SCORE
# -----------------------


@beartype
def compute_confidence_score(df: pl.DataFrame) -> pl.DataFrame:
    """
    Score de confiance :
    - 1.0 : texte exploitable
    - 0.3 : court, spam, non-latin, quasi-duplicat
    """

    return df.with_columns(
        pl.when(
            pl.col("is_short")
            | pl.col("is_spam")
            | pl.col("is_quasi_duplicate")
            | pl.col("is_non_latin")
            | (pl.col("clean_content").str.len_chars() < 10)
        )
        .then(0.3)
        .otherwise(1.0)
        .alias("confidence_score")
    )
