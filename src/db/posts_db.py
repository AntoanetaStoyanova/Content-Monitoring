import json
import os
import sys

from beartype import beartype
from dotenv import load_dotenv

from db.postgresql_connector import get_connection

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from bin.collect_posts import collect_bluesky_posts
from bin.log import logger

load_dotenv()

USERNAME_BLUESKY = os.getenv("USERNAME_BLUESKY")
PASSWORD_BLUESKY = os.getenv("PASSWORD_BLUESKY")


@beartype
def save_posts_db(
    n_keywords: int,
    n_post: int = 10,
    generate_keywords: bool = True,
    category: list[str] | None = None,
) -> None:
    """
    Collecte des posts depuis Bluesky pour une catégorie donnée et les sauvegarde
    dans les tables PostgreSQL `posts` et `post_keywords`.

    :param category: Nom de la catégorie.
    :param n_keywords: Nombre de mots-clés à générer pour la catégorie.
    :param n_post: Nombre maximum de posts à récupérer par mot-clé.
    """

    if generate_keywords and not category:
        raise ValueError("category is required when generate_keywords=True")

    # 1️⃣ Collect posts
    all_posts = collect_bluesky_posts(
        n_keywords=n_keywords,
        n_post=n_post,
        username=USERNAME_BLUESKY,
        password=PASSWORD_BLUESKY,
        generate_keywords=generate_keywords,
        category=category,
    )

    if not all_posts:
        if category:
            logger.warning(f"Aucun post collecté pour la catégorie '{category}'.")
        else:
            logger.warning("Aucun post collecté (toutes catégories).")
        return

    # 2️⃣ Insert posts into DB
    with get_connection() as conn:
        with conn.cursor() as cur:
            for post in all_posts:
                # Safely convert embed to JSON
                embed_obj = post.get("embed")
                if (
                    embed_obj
                    and hasattr(embed_obj, "model_dump")
                    and callable(embed_obj.model_dump)
                ):
                    embed_json = embed_obj.model_dump()
                else:
                    embed_json = None
                # Insert post, avoid duplicates via external_id
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
                        json.dumps(post.get("labels", [])),
                        json.dumps(embed_json),
                    ),
                )
                post_id = cur.fetchone()[0]

                # Insert into post_keywords if keyword_id exists
                keyword_id = post.get("keyword_id")
                if keyword_id:
                    cur.execute(
                        """
                        INSERT INTO bluesky.post_keywords (post_id, keyword_id)
                        VALUES (%s, %s)
                        ON CONFLICT (post_id, keyword_id) DO NOTHING;
                        """,
                        (post_id, keyword_id),
                    )

        conn.commit()

    if category:
        logger.info(
            f"✅ {len(all_posts)} posts sauvegardés pour la catégorie '{category}'."
        )
    else:
        logger.info(f"✅ {len(all_posts)} posts sauvegardés (toutes catégories).")
