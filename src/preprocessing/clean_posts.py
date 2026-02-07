import os
import sys

import faiss
import numpy as np
import polars as pl
from beartype import beartype
from sentence_transformers import SentenceTransformer

# sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))


from src.utils.cleaning_posts import (
    clean_text,
    compute_confidence_score,
    detect_language,
    detect_non_latin,
    detect_short_spam,
    extract_emojis,
    extract_hashtags,
)
from src.utils.log import logger
from src.utils.select_query import execute_query


@beartype
def clean_and_verify_posts(
    limit: int = 100,
    batch_size: int = 5000,
    similarity_threshold: float = 0.95,
) -> pl.DataFrame:
    """
    Nettoie les posts et prépare un dataset robuste pour
    fake news / analyse émotionnelle
    """

    # -----------------------
    # LOAD DATA
    # -----------------------
    query = f"SELECT id, external_id, content FROM bluesky.posts LIMIT {limit};"
    posts_df = execute_query(query, return_df=True)

    if posts_df is None or posts_df.is_empty():
        raise ValueError("Aucun post récupéré depuis la base")

    # -----------------------
    # SIGNAL SEPARATION
    # -----------------------
    posts_df = posts_df.with_columns(
        [
            pl.col("content")
            .map_elements(extract_hashtags, return_dtype=pl.List(pl.Utf8))
            .alias("hashtags"),
            pl.col("content")
            .map_elements(extract_emojis, return_dtype=pl.List(pl.Utf8))
            .alias("emojis"),
            pl.col("content")
            .map_elements(clean_text, return_dtype=pl.Utf8)
            .alias("clean_content"),
        ]
    )

    # -----------------------
    # BASIC FILTERS
    # -----------------------
    posts_df = detect_short_spam(posts_df)
    posts_df = detect_non_latin(posts_df)
    posts_df = posts_df.with_columns(
        pl.col("clean_content")
        .map_elements(detect_language, return_dtype=pl.Utf8)
        .alias("detected_language")
    )
    # pl.col("clean_content").map_elements(detect_language, return_dtype=pl.Utf8).alias(
    #     "detected_language"
    # )

    # -----------------------
    # DUPLICATE DETECTION
    # -----------------------
    model = SentenceTransformer("all-MiniLM-L6-v2")
    texts = posts_df["clean_content"].to_list()
    n_posts = len(texts)
    quasi_duplicates = set()

    for start in range(0, n_posts, batch_size):
        end = min(start + batch_size, n_posts)
        logger.info(f"Batch {start}-{end} / {n_posts}")

        batch_texts = texts[start:end]
        embeddings = model.encode(batch_texts, show_progress_bar=True)
        embeddings = np.asarray(embeddings, dtype="float32")
        faiss.normalize_L2(embeddings)

        index = faiss.IndexFlatIP(embeddings.shape[1])
        index.add(embeddings)

        D, I = index.search(embeddings, k=5)  # noqa: E741

        for i, (neighbors, sims) in enumerate(zip(I, D, strict=False)):
            for j, sim in zip(neighbors, sims, strict=False):
                if i != j and sim > similarity_threshold:
                    quasi_duplicates.add(start + i)
                    quasi_duplicates.add(start + j)

    posts_df = posts_df.with_columns(
        pl.Series(
            "is_quasi_duplicate",
            [i in quasi_duplicates for i in range(n_posts)],
        )
    )

    # -----------------------
    # CONFIDENCE SCORE
    # -----------------------
    posts_df = compute_confidence_score(posts_df)

    # -----------------------
    # SAVE
    # -----------------------
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    output_path = os.path.join(project_root, "data", "posts_cleaned.parquet")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    posts_df.write_parquet(output_path)

    logger.info(f"Dataset sauvegardé : {output_path}")

    return posts_df



