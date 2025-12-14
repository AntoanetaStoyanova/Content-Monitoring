from atproto import Client
import os
import polars as pl
from create_key_words import save_key_words_csv
from beartype import beartype
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict

from dotenv import load_dotenv
load_dotenv()

data_folder = os.path.join(os.getcwd(), "data")
os.makedirs(data_folder, exist_ok=True)
collect_post_csv_path = os.path.join(data_folder, "collected_post.csv")
key_words_csv_path = os.path.join(data_folder, "keywords_generated.csv")

# il faut créer .env 
USERNAME_BLUESKY = os.getenv("USERNAME_BLUESKY")
PASSWORD_BLUESKY = os.getenv("PASSWORD_BLUESKY")


@beartype
def collect_bluesky_posts(category: str, n_keywords: int, username: str, password: str, n_post: int) -> List[Dict[str, str]]:
    """
    Collecte des posts sur Bluesky contenant des mots-clés générés pour une catégorie donnée.

    Cette fonction :
    - Génère des mots-clés pour la catégorie via `save_key_words_csv`.
    - Récupère les mots-clés depuis le CSV local.
    - Se connecte à un compte Bluesky avec les identifiants fournis.
    - Recherche les posts contenant chaque mot-clé.
    - Retourne la liste complète des posts trouvés.

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

    :returns: Liste de dictionnaires contenant les informations des posts.
              Chaque dictionnaire contient les clés suivantes :
              - 'mot_cle' : le mot-clé utilisé pour la recherche.
              - 'auteur' : le handle de l'auteur du post.
              - 'texte' : le contenu textuel du post.
    :rtype: List[dict]

    :log info: Informations sur le nombre de posts trouvés pour chaque mot-clé.
    :log error: Erreurs survenues lors de la récupération des posts pour un mot-clé.

    :example:
        >>> posts = collect_bluesky_posts("politique", 5, "mon_user", "mon_mdp", 10)
        >>> len(posts)
        42
        >>> posts[0]
        {'mot_cle': 'gouvernement', 'auteur': 'user123', 'texte': 'Le gouvernement annonce...'}
    """

    # générer des mots clés
    save_key_words_csv(category=category, n_keywords=n_keywords)
    # récupérer les mots clés
    df_mot_cles = pl.read_csv(key_words_csv_path)
    list_mot_cles = df_mot_cles["keyword"].to_list()

    # identifiants account Bluesky
    client = Client()
    client.login(username, password)

    all_posts = []

    # Recherche des posts contenant les mots-clés
    def fetch_posts(mot_cle):
        try:
            res = client.app.bsky.feed.search_posts(
                params={
                    "q": mot_cle,
                    "limit": n_post
                }
            )
        
            # récupérer les posts pour le mot_clé
            posts = res.posts
            result = [{
                "mot_cle": mot_cle,
                "auteur": post.author.handle if post.author else None,
                "texte": post.record.text if post.record else None
            } for post in posts]
            print(f"[INFO] {len(posts)} posts trouvés pour '{mot_cle}'")
            return result
        except Exception as e:
            print(f"[ERREUR] Mot-clé '{mot_cle}': {e}")
            return []
        
    # Utilisation de threads pour accélérer la collecte
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
    all_posts = collect_bluesky_posts(category, n_keywords, USERNAME_BLUESKY, PASSWORD_BLUESKY, n_post)
    # Convertir en DataFrame pour analyse ou export
    df_posts = pl.DataFrame(all_posts)
    df_posts.write_csv(collect_post_csv_path)




if __name__ == "__main__":
    
    category = "environnement"
    n_keywords = 2
    save_posts_csv(category=category, n_keywords=n_keywords, n_post=2)