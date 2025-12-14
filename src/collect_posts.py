import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timezone
from typing import Dict, List

import polars as pl
from atproto import Client
from beartype import beartype
from dotenv import load_dotenv

from create_key_words import save_key_words_csv

load_dotenv()


data_folder = os.path.join(os.getcwd(), "data")
os.makedirs(data_folder, exist_ok=True)
collect_post_csv_path = os.path.join(data_folder, "collected_post.csv")
key_words_csv_path = os.path.join(data_folder, "keywords_generated.csv")

# il faut créer .env
USERNAME_BLUESKY = os.getenv("USERNAME_BLUESKY")
PASSWORD_BLUESKY = os.getenv("PASSWORD_BLUESKY")


@beartype
def collect_bluesky_posts(
    category: str, n_keywords: int, username: str, password: str, n_post: int
) -> List[Dict[str, str]]:
    """
    Collecte des posts depuis Bluesky contenant des mots-clés générés pour une
    catégorie donnée.

    La fonction :
    - Génère des mots-clés via `save_key_words_csv`.
    - Lit les mots-clés générés correspondant à l'utilisateur.
    - Se connecte à un compte Bluesky avec les identifiants fournis.
    - Recherche des posts contenant chaque mot-clé.

    :param category: Nom de la catégorie pour laquelle générer et rechercher des posts.
    :type category: str
    :param n_keywords: Nombre de mots-clés à générer pour la catégorie.
    :type n_keywords: int
    :param username: Nom d'utilisateur du compte Bluesky.
    :type username: str
    :param password: Mot de passe du compte Bluesky.
    :type password: str
    :param n_post: Nombre maximum de posts à récupérer par mot-clé.
    :type n_post: int

    :return: Liste de dictionnaires représentant les posts.
    :rtype: List[Dict[str, str]]

    :raises Exception: Si une erreur se produit lors de la récupération des posts.

    .. note::
        La récupération de la date réelle dépend de la disponibilité du champ `createdAt`
        dans le record du post. Si le SDK ne fournit pas cette information, la date est
        générée automatiquement au moment du scraping.

    :example:

    >>> posts = collect_bluesky_posts("technologie", 3, "mon_user", "mon_mdp", 5)
    >>> len(posts) <= 15
    True
    >>> posts[0].keys()
    dict_keys(['mot_cle', 'auteur', 'texte', 'date'])
    >>> isinstance(posts[0]['date'], str)
    True
    """

    # Générer des mots clés
    save_key_words_csv(category=category, n_keywords=n_keywords)

    # Lire que les mots-clés qui provient d'user query
    df_mot_cles = pl.read_csv(key_words_csv_path).tail(n_keywords)
    list_mot_cles = df_mot_cles["keyword"].to_list()

    # Connexion Bluesky
    client = Client()
    client.login(username, password)

    all_posts = []

    def fetch_posts(mot_cle):
        try:
            res = client.app.bsky.feed.search_posts(
                params={"q": mot_cle, "limit": n_post}
            )
            posts = res.posts
            result = []

            for post in posts:
                texte = post.record.text if post.record else None
                auteur = post.author.handle if post.author else None
                created_at = None

                # Récupérer le record brut si disponible
                try:
                    if post.record:
                        record_dict = getattr(
                            post.record, "model_dump", lambda: post.record
                        )()
                        # Essayer createdAt
                        created_at = record_dict.get("createdAt") or record_dict.get(
                            "created_at"
                        )
                except Exception:
                    created_at = None

                # Si aucune date, créer la date au moment du scraping
                if not created_at:
                    from datetime import datetime

                    created_at = datetime.now(timezone.utc).isoformat()

                result.append(
                    {
                        "mot_cle": mot_cle,
                        "auteur": auteur,
                        "texte": texte,
                        "date": created_at,
                    }
                )

            print(f"[INFO] {len(posts)} posts trouvés pour '{mot_cle}'")
            return result

        except Exception as e:
            print(f"[ERREUR] Mot-clé '{mot_cle}': {e}")
            return []

    # Multithreading pour accélérer
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(fetch_posts, mot_cle) for mot_cle in list_mot_cles]
        for future in as_completed(futures):
            all_posts.extend(future.result())

    return all_posts


@beartype
def save_posts_csv(category: str, n_keywords: int, n_post: int = 10):
    """
    Collecte des posts depuis Bluesky pour une catégorie donnée et les sauvegarde dans un fichier CSV.

    Cette fonction :
    - Utilise `collect_bluesky_posts` pour récupérer les posts correspondant aux mots-clés générés.
    - Convertit la liste de posts en DataFrame Polars.
    - Sauvegarde la DataFrame dans le fichier CSV spécifié par `collect_post_csv_path`.

    :param category: Nom de la catégorie pour laquelle récupérer les posts.
    :type category: str
    :param n_keywords: Nombre de mots-clés à générer pour la catégorie.
    :type n_keywords: int
    :param n_post: Nombre maximum de posts à récupérer par mot-clé. Par défaut 10.
    :type n_post: int, optional

    :returns: None
    :rtype: None

    :example:
        >>> save_posts_csv("politique", 5, 10)
        # Le fichier CSV 'collect_post_csv_path' est créé ou mis à jour avec les posts collectés.
    """
    all_posts = collect_bluesky_posts(
        category, n_keywords, USERNAME_BLUESKY, PASSWORD_BLUESKY, n_post
    )
    # Convertir en DataFrame pour analyse ou export
    df_posts = pl.DataFrame(all_posts)
    df_posts.write_csv(collect_post_csv_path)


if __name__ == "__main__":
    category = "Caisse d'épargne"
    n_keywords = 10
    save_posts_csv(category=category, n_keywords=n_keywords, n_post=10)
