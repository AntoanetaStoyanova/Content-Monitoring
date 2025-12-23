import os
import sys
import time

import psycopg2
from beartype import beartype

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from bin.create_key_words import generate_keywords
from bin.log import logger


@beartype
def save_key_words_db(
    categories: list[str],
    n_keywords: int,
    conn: psycopg2.extensions.connection,
    models: list[str],
) -> None:
    """
    Génère des mots-clés pour une liste de catégories et les insère dans les tables
    PostgreSQL `categories` et `keywords`. Évite les doublons.

    :param categories: Liste de catégories à traiter
    :param n_keywords: Nombre de mots-clés à générer par catégorie
    :param conn: Connexion PostgreSQL active
    :param models: Liste des modèles à utiliser pour la génération
    """
    cur = conn.cursor()

    # 1️⃣ Récupérer les catégories existantes
    cur.execute("SELECT category, id FROM categories;")
    existing = cur.fetchall()
    existing_categories = {c[0]: c[1] for c in existing}

    # 2️⃣ Filtrer les nouvelles catégories
    new_categories = [c for c in categories if c not in existing_categories]

    if not new_categories:
        msg = "✅ Toutes les catégories existent déjà dans la base. Rien à ajouter."
        print(msg)
        logger.info(msg)
        cur.close()
        return

    logger.info(f"📌 Nouvelles catégories à traiter : {new_categories}")

    # 3️⃣ Insérer les nouvelles catégories
    for cat in new_categories:
        cur.execute(
            "INSERT INTO categories (category) VALUES (%s) RETURNING id;", (cat,)
        )
        cat_id = cur.fetchone()[0]
        existing_categories[cat] = cat_id

    conn.commit()

    # 4️⃣ Générer les mots-clés pour les nouvelles catégories
    all_keywords = []
    for model in models:
        logger.info(f"🤖 Utilisation du modèle : {model}")
        try:
            results = generate_keywords(
                queries=new_categories, model=model, n_keywords=n_keywords
            )
            for result in results:
                cat = result["query"]
                cat_id = existing_categories[cat]
                for kw in result["keywords"]:
                    all_keywords.append((cat_id, kw["keyword"], kw["language"]))
        except Exception as e:
            logger.error(f"⚠️ Problème avec le modèle {model} : {e}")
        time.sleep(1)

    # 5️⃣ Insérer les mots-clés dans la table keywords
    for cat_id, keyword, language in all_keywords:
        # Utiliser INSERT ... ON CONFLICT pour éviter les doublons
        cur.execute(
            """
            INSERT INTO keywords (category_id, keyword, language)
            VALUES (%s, %s, %s)
            ON CONFLICT (category_id, keyword, language) DO NOTHING;
        """,
            (cat_id, keyword, language),
        )

    conn.commit()
    cur.close()

    msg = f"✅ {len(all_keywords)} mots-clés ajoutés pour {len(new_categories)} nouvelles catégories."
    print(msg)
    logger.info(msg)
