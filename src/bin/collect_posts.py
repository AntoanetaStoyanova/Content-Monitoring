import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC
from typing import Any

from atproto import Client
from beartype import beartype

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from db.categories_keywords import save_key_words_db
from db.postgresql_connector import get_connection

UTC = UTC
models = ["mistral:7b"]


@beartype
def collect_bluesky_posts(
    n_keywords: int,
    username: str,
    password: str,
    n_post: int,
    delay_between_requests: float = 3.0,
    generate_keywords: bool = True,
    category: list[str] | None = None,
) -> list[dict[str, Any]]:
    """ """
    if generate_keywords and not category:
        raise ValueError("category is required when generate_keywords=True")

    # generate keywords=True
    if generate_keywords:
        with get_connection() as conn:
            save_key_words_db(
                categories=category,
                n_keywords=n_keywords,
                conn=conn,
                models=models,
            )

    # Ouvrir la connexion PostgreSQL
    with get_connection() as conn:
        with conn.cursor() as cur:
            if category:
                if len(category) == 1:
                    cur.execute(
                        """
                        SELECT k.id, k.keyword, k.language
                        FROM keywords AS k
                        JOIN categories AS c ON k.category_id = c.id
                        WHERE c.category = %s
                        LIMIT %s;
                        """,
                        (category[0], n_keywords),
                    )
                else:
                    # multiple categories
                    cur.execute(
                        """
                        SELECT k.id, k.keyword, k.language
                        FROM keywords AS k
                        JOIN categories AS c ON k.category_id = c.id
                        WHERE c.category = ANY(%s)
                        LIMIT %s;
                        """,
                        (category, n_keywords),
                    )
            else:
                cur.execute(
                    """
                    SELECT k.id, k.keyword, k.language
                    FROM keywords k
                    LIMIT %s;
                    """,
                    (n_keywords,),
                )
            rows = cur.fetchall()

    if not rows:
        return []
    keyword_map = {r[1]: {"id": r[0], "language": r[2]} for r in rows}

    # Connexion Bluesky
    client = Client()
    client.login(username, password)

    all_posts: list[dict[str, Any]] = []

    def fetch_posts(mot_cle: str) -> list[dict[str, Any]]:
        try:
            time.sleep(delay_between_requests)
            res = client.app.bsky.feed.search_posts(
                params={"q": mot_cle, "limit": n_post}
            )
            result = []

            for post in res.posts:
                record_dict = {}
                if post.record and hasattr(post.record, "model_dump"):
                    record_dict = post.record.model_dump()

                content = record_dict.get("text") if record_dict else None

                # Skip if content is missing
                if not content:
                    continue

                result.append(
                    {
                        "keyword": mot_cle,
                        "keyword_id": keyword_map[mot_cle]["id"]
                        if mot_cle in keyword_map
                        else None,
                        "language": keyword_map[mot_cle]["language"]
                        if mot_cle in keyword_map
                        else None,
                        "content": content,
                        "external_id": getattr(post, "uri", None),
                        "created_at": record_dict.get("createdAt")
                        or record_dict.get("created_at")
                        or "1970-01-01T00:00:00Z",
                        "like_count": getattr(post, "like_count", 0),
                        "reply_count": getattr(post, "reply_count", 0),
                        "quote_count": getattr(post, "quote_count", 0),
                        "repost_count": getattr(post, "repost_count", 0),
                        "labels": getattr(post, "labels", []),
                        "embed": getattr(post, "embed", None),
                    }
                )
            return result

        except Exception as e:
            print(f"[ERREUR] Mot-clé '{mot_cle}': {e}")
            return []

    # creation d'un thread pool qui manage combient de nb tasks peuvent s'executer
    with ThreadPoolExecutor(max_workers=1) as executor:
        futures = [executor.submit(fetch_posts, kw) for kw in keyword_map.keys()]
        for future in as_completed(futures):
            all_posts.extend(future.result())

    # Déduplication par (content, external_id)
    seen = set()
    unique_posts: list[dict[str, Any]] = []

    for post in all_posts:
        key = (post["content"], post["external_id"])
        if key not in seen:
            seen.add(key)
            unique_posts.append(post)

    return unique_posts
