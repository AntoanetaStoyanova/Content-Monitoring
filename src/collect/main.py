import os
import random
import sys
import time

from atproto import Client
from collect_posts import collect_bluesky_posts
from dotenv import load_dotenv
from posts_db import insert_posts_to_db

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from bin.log import logger
from db.postgresql_connector import get_connection

load_dotenv()


def main():
    """
    Cette fonction orchestre le processus de collecte :
    Elle se connecte à l’API Bluesky, récupère les mots-clés, collecte les posts
    par mot-clé en paginant, évite les doublons et enregistre uniquement les posts
    pertinents en base.

    Le traitement est effectué mot-clé par mot-clé afin de répartir la
    charge sur l’API et de faciliter le suivi des volumes collectés.

    :return: ``None``. Cette fonction déclenche un traitement complet
            de collecte et d’enregistrement des posts.
    :rtype: None

    :raises Exception: Toute exception levée lors de l’authentification,
                    des appels API ou des opérations en base de données
                    est interceptée, loggée, et provoque l’arrêt du
                    traitement du mot-clé courant.
    """
    # instance du client API Bluesky avec les identifiants d'autentification
    client = Client()
    client.login(os.getenv("USERNAME_BLUESKY"), os.getenv("PASSWORD_BLUESKY"))

    # Récupérer tous les mots-clés à traiter
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT keyword FROM bluesky.keywords")
            # transforme le résultat SQL en liste Python de chaînes
            all_keywords = [r[0] for r in cur.fetchall()]

    # vérifie qu'au moin un mot-clé existe
    if not all_keywords:
        logger.error("❌ Aucune mot-clé en base.")
        return

    # compte le nombre de posts colléctés sur tous les mot-clés
    total_global = 0

    # On boucle sur chaque mot-clé un par un
    for kw in all_keywords:
        logger.info(f"🚀 Début de collecte pour le mot : {kw.upper()}")
        # None = 1ère page
        current_cursor = None
        # mot-cké collécrés pour le mot-clé
        kw_collected = 0

        # on pagine max 10 pages par mot-clés, évite de surconsommer API
        for _page in range(10):
            try:
                # appel de la collecte
                matched, scanned, next_cursor = collect_bluesky_posts(
                    client, kw, current_cursor
                )

                # marque les posts comme déjà scannés pour éviter de les re-collecter
                if scanned:
                    with get_connection() as conn:
                        with conn.cursor() as cur:
                            cur.executemany(
                                "INSERT INTO bluesky.scanned_posts (external_id) VALUES (%s) ON CONFLICT DO NOTHING",
                                [(i,) for i in scanned],
                            )
                        conn.commit()

                # sauvegarde des posts scannés
                if matched:
                    insert_posts_to_db(matched)
                    total_global += len(matched)
                    kw_collected += len(matched)

                # gestion de la pagination
                current_cursor = next_cursor
                if not current_cursor:
                    break

                time.sleep(random.uniform(2, 4))

            except Exception as e:
                logger.error(f"Erreur : {e}")
                break

        logger.info(
            f"✅ Terminé pour '{kw}' : {kw_collected} récoltés. Total Global: {total_global}"
        )

    logger.info("🏁 Tous les mots-clés ont été traités.")


if __name__ == "__main__":
    main()
