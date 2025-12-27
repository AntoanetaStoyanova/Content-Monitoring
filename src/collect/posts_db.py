import json
import os
import sys

from beartype import beartype

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from bin.log import logger
from db.postgresql_connector import get_connection


@beartype
def insert_posts_to_db(posts: list[dict], commit=True):
    """
    Les posts sont insérés ou mis à jour grâce à la clause
    ``ON CONFLICT (external_id)``.
    Les champs complexes (labels et embed) sont convertis en JSON avant
    l’insertion.

    Elle gère également la relation plusieurs-à-plusieurs entre les posts
    et les mots-clés via la table ``bluesky.post_keywords``.

    :param posts: Liste de dictionnaires représentant des posts Bluesky
        normalisés, incluant leurs métadonnées et mots-clés associés.
    :type posts: list[dict]

    :param commit: Indique si la transaction doit être validée après
        insertion du batch. Utile pour les tests ou les traitements par lots.
    :type commit: bool

    :return: ``None``. La fonction effectue uniquement des opérations
        écriture en base de données.
    :rtype: None

    :raises Exception: Toute erreur liée à la base de données ou à la
        sérialisation des données est interceptée, loggée et provoque un rollback
        de la transaction.
    """
    # vérification d'entrée
    if not posts:
        logger.warning("⚠️ No posts to insert.")
        return

    try:
        # connexion à la base de données
        with get_connection() as conn:
            with conn.cursor() as cur:
                for post in posts:
                    embed_obj = post.get("embed")
                    # transforme l'objet embed en données JSON sérialisables
                    embed_json = (
                        embed_obj.model_dump()
                        if embed_obj and hasattr(embed_obj, "model_dump")
                        else embed_obj
                    )

                    labels_obj = post.get("labels", [])
                    labels_json = [
                        lbl.model_dump() if hasattr(lbl, "model_dump") else str(lbl)
                        for lbl in labels_obj
                    ]

                    # insertion du post dans la table posts
                    # si un post avec le même external_id exist, il est mit à jour
                    cur.execute(
                        """
                        INSERT INTO bluesky.posts (
                            external_id, content, language, created_at, like_count,
                            reply_count, quote_count, repost_count, labels, embed
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (external_id) DO UPDATE SET
                            content = EXCLUDED.content,
                            language = EXCLUDED.language,
                            created_at = EXCLUDED.created_at,
                            like_count = EXCLUDED.like_count,
                            reply_count = EXCLUDED.reply_count,
                            quote_count = EXCLUDED.quote_count,
                            repost_count = EXCLUDED.repost_count,
                            labels = EXCLUDED.labels,
                            embed = EXCLUDED.embed
                        RETURNING id;
                    """,
                        (
                            post.get("external_id"),
                            post.get("content"),
                            post.get("language"),
                            post.get("created_at"),
                            post.get("like_count", 0),
                            post.get("reply_count", 0),
                            post.get("quote_count", 0),
                            post.get("repost_count", 0),
                            json.dumps(labels_json),
                            json.dumps(embed_json),
                        ),
                    )
                    post_id = cur.fetchone()[0]

                    # Insert keyword mapping
                    for keyword_id in post.get("keyword_ids", []):
                        cur.execute(
                            """
                            INSERT INTO bluesky.post_keywords (post_id, keyword_id)
                            VALUES (%s, %s)
                            ON CONFLICT DO NOTHING;
                        """,
                            (post_id, keyword_id),
                        )

            if commit:
                conn.commit()
        logger.info(f"✅ Batch of {len(posts)} posts inserted into DB successfully.")
    except Exception as e:
        logger.error(f"❌ Failed to insert batch into DB: {e}")
        if conn:
            conn.rollback()
