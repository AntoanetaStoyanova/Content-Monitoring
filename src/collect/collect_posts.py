import os
import random
import re
import sys
import time

from atproto import Client
from beartype import beartype

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from bin.log import logger
from db.postgresql_connector import get_connection


@beartype
def collect_bluesky_posts(
    client: Client,
    target_keyword: str,  # On lui passe un mot précis
    cursor: str | None = None, # marque-page pour la pagination
    delay_between_requests: tuple[float, float] = (1.0, 2.0),
) -> tuple[list[dict], list[str], str | None]:
    """
    Cette fonction interroge API Bluesky afin de récupérer des posts contenant
    un mot-clé donné. Elle évite les doublons en se basant sur les identifiants
    déjà stockés en base de données, applique une double validation (recherche API
    + filtrage RegEx), et gère la pagination via un curseur.

    :param client: Client Bluesky utilisé pour interroger API.
    :type client: Client

    :param target_keyword: Mot-clé exact à rechercher dans les posts.
    :type target_keyword: str

    :param cursor: Curseur de pagination fourni par API Bluesky.
        ``None`` indique une première requête.
    :type cursor: str | None

    :param delay_between_requests: Intervalle (min, max) en secondes entre deux
        requêtes API afin de respecter les limites
                                de taux.
    :type delay_between_requests: tuple[float, float]

    :return: Un triplet contenant :
        - la liste des posts correspondant réellement au mot-clé,
        - la liste des identifiants de posts scannés durant cet appel,
        - le curseur de pagination pour appel suivant (ou ``None``).
    :rtype: tuple[list[dict], list[str], str | None]

    :raises Exception: Toute exception liée à API, à la base de données ou au
        traitement est interceptée et loggée.
    """

    # connecion à la base de données
    with get_connection() as conn:
        with conn.cursor() as cur:
            # éviter les doublons
            cur.execute("SELECT external_id FROM bluesky.scanned_posts")
            scanned_ids_set = {r[0] for r in cur.fetchall()}

            # On récupère les infos du mot-clé (id et langue)
            cur.execute(
                "SELECT id, language FROM bluesky.keywords WHERE keyword = %s",
                (target_keyword,),
            )
            row = cur.fetchone()
            if not row:
                return [], [], None
            kw_id, lang = row

    # Préparation du pattern RegEx pour valider le post
    # ai ne matchera pas aimé
    pattern = re.compile(rf"\b{re.escape(target_keyword)}\b", re.IGNORECASE)

    matched_posts = []
    scanned_batch_ids = []

    try:
        params = {"q": target_keyword, "limit": 100}
        # récupérer la page suivant si cursor
        if cursor:
            params["cursor"] = cursor

        # appel à l'API Bluesky
        res = client.app.bsky.feed.search_posts(params=params)
        # récupère la list des posts, si posts n'existe pas la liste est vide
        posts_found = getattr(res, "posts", [])

        for post in posts_found:
            uri = post.uri
            if uri in scanned_ids_set:
                continue

            scanned_ids_set.add(uri)
            scanned_batch_ids.append(uri)

            # récupère le text du post
            text = getattr(post.record, "text", "").lower()

            # vérifie que le mot_clé est présent dans le text
            if pattern.search(text):
                # construction du dictionnaire post
                matched_posts.append(
                    {
                        "external_id": uri,
                        "content": text,
                        "created_at": getattr(post.record, "createdAt", None),
                        "like_count": getattr(post, "like_count", 0) or 0,
                        "reply_count": getattr(post, "reply_count", 0) or 0,
                        "quote_count": getattr(post, "quote_count", 0) or 0,
                        "repost_count": getattr(post, "repost_count", 0) or 0,
                        "labels": getattr(post, "labels", []),
                        "embed": getattr(post, "embed", None),
                        "language": lang,
                        "keyword_ids": {kw_id},
                    }
                )

        logger.info(
            f"🔎 [{target_keyword}] -> Reçu: {len(posts_found)} | Matchs: {len(matched_posts)}"
        )

        # pause aléatoire pour respecter les limites API
        time.sleep(random.uniform(*delay_between_requests))
        return matched_posts, scanned_batch_ids, getattr(res, "cursor", None)

    except Exception as e:
        logger.error(f"❌ Erreur sur '{target_keyword}': {e}")
        return [], [], None


# @beartype
# def collect_bluesky_posts(
#     client: Client,
#     feed_func=None,
#     delay_between_requests: tuple[float, float] = (1.0, 2.0),  # délai interne par feed
# ) -> tuple[list[dict], list[str]]:
#     """Collecte les posts récents, dédupliqués via scanned_posts."""
#     with get_connection() as conn:
#         with conn.cursor() as cur:
#             cur.execute("SELECT external_id FROM bluesky.scanned_posts")
#             scanned_ids_set = {r[0] for r in cur.fetchall()}

#             cur.execute("SELECT id, keyword, language FROM bluesky.keywords")
#             keywords = [
#                 {"id": i, "keyword": k.lower(), "language": l}
#                 for i, k, l in cur.fetchall()
#             ]

#     if not keywords:
#         return [], []

#     expanded_keywords = []
#     for k in keywords:
#         expanded_keywords.extend(
#             [
#                 k,
#                 {**k, "keyword": generate_plural(k["keyword"], k["language"])},
#                 {**k, "keyword": get_lemma(k["keyword"], k["language"])},
#             ]
#         )

#     for k in expanded_keywords:
#         k["pattern"] = re.compile(rf"\b{re.escape(k['keyword'])}\b", re.IGNORECASE)

#     matched_posts = []
#     scanned_batch_ids = []

#     # Si feed_func est une liste de lambdas, on itère sur chaque feed
#     if isinstance(feed_func, list):
#         for f in feed_func:
#             try:
#                 res = f(client)
#             except Exception as e:
#                 print(f"❌ Error fetching feed: {e}")
#                 continue

#             for view in res.feed:
#                 post = view.post
#                 uri = post.uri
#                 if uri in scanned_ids_set:
#                     continue
#                 scanned_ids_set.add(uri)
#                 scanned_batch_ids.append(uri)

#                 text = getattr(post.record, "text", "").lower()
#                 base_post = {
#                     "external_id": uri,
#                     "content": text,
#                     "created_at": getattr(post.record, "createdAt", None),
#                     "like_count": post.like_count or 0,
#                     "reply_count": post.reply_count or 0,
#                     "quote_count": post.quote_count or 0,
#                     "repost_count": post.repost_count or 0,
#                     "labels": str(getattr(post, "labels", [])),
#                 }

#                 matches = [k for k in expanded_keywords if k["pattern"].search(text)]
#                 if matches:
#                     matched_posts.append(
#                         {
#                             **base_post,
#                             "language": matches[0]["language"],
#                             "keyword_ids": {m["id"] for m in matches},
#                         }
#                     )

#             # 🔹 Petit délai entre les appels API pour éviter throttling

#             time.sleep(random.uniform(*delay_between_requests))

#     else:
#         # Si feed_func est une seule lambda
#         res = (
#             feed_func(client)
#             if feed_func
#             else client.app.bsky.feed.get_timeline(params={"limit": 50})
#         )
#         for view in res.feed:
#             post = view.post
#             uri = post.uri
#             if uri in scanned_ids_set:
#                 continue
#             scanned_ids_set.add(uri)
#             scanned_batch_ids.append(uri)

#             text = getattr(post.record, "text", "").lower()
#             base_post = {
#                 "external_id": uri,
#                 "content": text,
#                 "created_at": getattr(post.record, "createdAt", None),
#                 "like_count": post.like_count or 0,
#                 "reply_count": post.reply_count or 0,
#                 "quote_count": post.quote_count or 0,
#                 "repost_count": post.repost_count or 0,
#                 "labels": str(getattr(post, "labels", [])),
#             }

#             matches = [k for k in expanded_keywords if k["pattern"].search(text)]
#             if matches:
#                 matched_posts.append(
#                     {
#                         **base_post,
#                         "language": matches[0]["language"],
#                         "keyword_ids": {m["id"] for m in matches},
#                     }
#                 )

#         time.sleep(random.uniform(*delay_between_requests))

#     return matched_posts, scanned_batch_ids
