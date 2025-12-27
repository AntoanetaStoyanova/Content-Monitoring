import os
import sys
from time import sleep

from beartype import beartype
from psycopg2.extras import execute_values

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from bin.log import logger
from generator_keywords.create_key_words import generate_keywords
from src.db.postgresql_connector import get_connection


@beartype
def save_key_words_db(
    categories: list[str],
    n_keywords: int,
    models: list[str],
) -> None:
    """
    Génère et sauvegarde des mots-clés uniques pour des catégories données.

    Cette fonction :
    1. Vérifie et crée les catégories manquantes dans la table `categories`.
    2. Récupère les mots-clés déjà existants pour éviter les doublons.
    3. Utilise différents modèles d'IA pour générer des mots-clés par catégorie.
    4. Filtre les mots-clés générés (longueur minimale, non-duplication).
    5. Insère uniquement les nouveaux mots-clés dans la table `keywords`.

    :param categories: Liste des catégories pour lesquelles générer des mots-clés.
    :type categories: list[str]

    :param n_keywords: Nombre de mots-clés à générer par catégorie et par modèle.
    :type n_keywords: int

    :param models: Liste des modèles d'IA utilisés pour la génération de mots-clés.
    :type models: list[str]

    :return: Cette fonction ne retourne rien. Elle effectue uniquement des écritures en
            base.
    :rtype: None

    :raises Exception: Toute erreur rencontrée lors de la connexion à la base,
                       de la génération ou de l'insertion des mots-clés est
                       loggée et relancée.
    """
    try:
        # connexion à la base de données
        with get_connection() as conn:
            with conn.cursor() as cur:
                # gestion des Catégories
                cur.execute("SELECT category, id FROM categories;")
                existing_categories = {c[0]: c[1] for c in cur.fetchall()}

                for cat in categories:
                    if cat not in existing_categories:
                        cur.execute(
                            "INSERT INTO categories (category) VALUES (%s) RETURNING id;",  # noqa: E501
                            (cat,),
                        )
                        existing_categories[cat] = cur.fetchone()[0]
                conn.commit()

                # Récupération des mots-clés existants pour éviter les doublons
                cur.execute("SELECT category_id, keyword FROM keywords;")
                db_existing_kws = {
                    (row[0], row[1].lower().strip()) for row in cur.fetchall()
                }

                # génération des mot-clés par modèle
                for model in models:
                    logger.info(f"🤖 Modèle : {model}")
                    results = generate_keywords(
                        queries=categories, model=model, n_keywords=n_keywords
                    )

                    # Pour dédoublonner ce que l'IA génère à l'instant
                    unique_batch = set()

                    for res in results:
                        cat_id = existing_categories[res["query"]]
                        for kw in res["keywords"]:
                            word = kw["keyword"].lower().strip()
                            lang = kw["language"]

                            # vérifie la longueur si c'est déjà en base
                            if (
                                len(word) >= 3
                            ):  # On ignore les mots de moins de 3 lettres
                                if (cat_id, word) not in db_existing_kws:
                                    unique_batch.add((cat_id, word, lang))

                    # insertion des nouveaux mots uniquement
                    if unique_batch:
                        all_to_insert = list(unique_batch)
                        execute_values(
                            cur,
                            "INSERT INTO keywords (category_id, keyword, language) VALUES %s;",  # noqa: E501
                            all_to_insert,
                        )
                        conn.commit()

                        # Mise à jour du set local pour que le prochain modèle
                        # ne ré-insère pas les mêmes mots que celui-ci
                        for item in all_to_insert:
                            db_existing_kws.add((item[0], item[1]))

                        logger.info(
                            f"💾 {len(all_to_insert)} nouveaux mots-clés uniques insérés."  # noqa: E501
                        )
                    else:
                        logger.info(f"✨ Aucun nouveau mot-clé à ajouter pour {model}.")

                    sleep(1)

    except Exception as e:
        logger.error(f"❌ Erreur : {e}")
        raise
