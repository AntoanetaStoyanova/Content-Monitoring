import json
import os
# import sys
from collections.abc import Iterable

import ollama
from beartype import beartype

# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from .utils import clean_keywords

from .log import logger

# partie qui remplace pour ce moment la base de données PostgreSQL
# dossier data
data_folder = os.path.join(os.getcwd(), "data")
os.makedirs(data_folder, exist_ok=True)
# csv pour les mots-clés
csv_path = os.path.join(data_folder, "keywords_generated.csv")

# à voir si on utilisera qu'un modèle
models = ["mistral:7b"]

SYSTEM_PROMPT_TOPIC = (
    "Tu es un modèle NLP professionnel. "
    "Résume le sujet principal de la phrase suivante en 1 à 3 mots. "
    "N'utilise un seul mot que si cela est suffisant pour le sens. "
    "Répond STRICTEMENT avec le ou les mots en minuscules, sans ponctuation."
)


@beartype
def extract_main_topic(queries: Iterable[str], model: str) -> list[str]:
    topics: list[str] = []

    for query in queries:
        response = ollama.chat(
            model=model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT_TOPIC},
                {"role": "user", "content": query},
            ],
        )

        topic = response.get("message", {}).get("content", "").strip().lower()

        if topic:
            topics.append(topic)
        else:
            topics.append("")
            logger.warning(f"⚠️ Sujet principal vide pour : {query}")

    return topics


@beartype
def generate_keywords(
    queries: list[str],
    model: str,
    n_keywords: int = 10,
) -> list[dict]:
    results: list[dict] = []

    main_topics = extract_main_topic(queries, model)

    for query, main_topic in zip(queries, main_topics, strict=False):
        if not main_topic:
            logger.warning(f"⚠️ Sujet principal vide pour : {query}")
            continue

        logger.info(f"🎯 Sujet principal : {main_topic}")

        prompt = f"""
        Génère EXACTEMENT {n_keywords} mots-clés pertinents autour du sujet :
        "{main_topic}"

        Contraintes STRICTES :
        - Format JSON uniquement
        - Structure :
        [
            {{"keyword": "...", "language": "fr"}},
            {{"keyword": "...", "language": "en"}}
        ]
        - Mots-clés en minuscules
        - Langue : fr ou en uniquement
        - Pas de texte hors JSON
        """

        response = ollama.chat(
            model=model,
            messages=[{"role": "user", "content": prompt}],
        )

        content = response.get("message", {}).get("content", "")

        try:
            parsed = json.loads(content)
        except json.JSONDecodeError:
            logger.error(f"❌ JSON invalide pour '{main_topic}' → {content[:200]}")
            continue

        clean_list = clean_keywords(parsed)

        if not clean_list:
            logger.warning(f"⚠️ Aucun mot-clé valide pour '{main_topic}'")
            continue

        results.append(
            {
                "query": query,
                "topic": main_topic,
                "keywords": clean_list,
            }
        )

        logger.info(f"✅ {len(clean_list)} mots-clés générés")

    return results
