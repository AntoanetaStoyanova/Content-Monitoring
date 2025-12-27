import json
from collections.abc import Iterable

import ollama
from beartype import beartype

from ..bin.log import logger
from ..bin.utils import clean_keywords

SYSTEM_PROMPT_TOPIC = (
    "Tu es un modèle NLP professionnel. "
    "Résume le sujet principal de la phrase suivante en 1 mot, "
    "comme le ferait un humain sur les réseaux sociaux. "
    "Répond STRICTEMENT avec le mot en minuscules, sans ponctuation."
)


def extract_main_topic(queries: Iterable[str], model: str) -> list[str]:
    """
    Extrait le sujet principal de chaque requête en un seul mot.

    Cette fonction utilise un modèle NLP via Ollama pour résumer
    chaque catégorie ou phrase passée dans `queries` en un seul mot
    représentatif, comme le ferait un humain sur les réseaux sociaux.
    La sortie est renvoyée sous forme de liste de mots en minuscules,
    dans le même ordre que les requêtes fournies.

    :param queries: Itérable de chaînes de caractères représentant
                    les catégories ou phrases à analyser.
    :type queries: Iterable[str]

    :param model: Nom du modèle NLP à utiliser pour l'extraction.
    :type model: str

    :return: Liste des sujets principaux extraits, un mot par requête.
             Si une erreur survient ou le compte de sujets ne correspond
             pas, la liste originale est renvoyée en minuscules.
    :rtype: list[str]

    :raises Exception: Toute erreur de traitement ou d'accès au modèle
                       est interceptée et loggée, mais ne bloque pas
                       la fonction (renvoie la liste originale).
    """
    # On transforme l'itérable en liste pour pouvoir compter
    query_list = list(queries)
    if not query_list:
        return []

    logger.info(
        f"⚡ Extraction groupée des sujets pour {len(query_list)} catégories..."
    )

    # On prépare une liste numérotée pour aider le modèle
    formatted_list = "\n".join([f"{i + 1}. {q}" for i, q in enumerate(query_list)])

    prompt = f"""
    Voici une liste de catégories provenant de réseaux sociaux. 
    Pour chaque catégorie, extrait le sujet principal en 1 seul mot (en minuscules).

    LISTE :
    {formatted_list}

    RÉPONDS STRICTEMENT AU FORMAT JSON SUIVANT :
    {{
        "topics": ["sujet1", "sujet2", "sujet3", ...]
    }}
    """

    try:
        response = ollama.chat(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            options={"temperature": 0},  # On veut de la précision, pas de créativité
        )

        content = response.get("message", {}).get("content", "").strip()

        # Nettoyage du JSON au cas où
        if "```" in content:
            content = content.split("```")[1].replace("json", "").strip()

        data = json.loads(content)
        topics = data.get("topics", [])

        # Sécurité : on vérifie que le compte est bon
        if len(topics) != len(query_list):
            logger.warning(
                f"⚠️ Décalage : {len(topics)} sujets extraits pour {len(query_list)} requêtes."  # noqa: E501
            )
            # Si le compte n'est pas bon, on renvoie les queries originales par défaut
            return [q.lower() for q in query_list]

        return topics

    except Exception as e:
        logger.error(f"❌ Erreur extraction groupée : {e}")
        # En cas d'échec, on retourne la liste originale en minuscules
        return [q.lower() for q in query_list]


@beartype
def generate_keywords(
    queries: list[str],
    model: str,
    n_keywords: int = 10,
) -> list[dict]:
    """
    Génère des mots-clés uniques pour chaque requête donnée.

    Cette fonction :
    1. Extrait le sujet principal de chaque requête via `extract_main_topic`.
    2. Utilise un modèle NLP pour générer une liste de mots-clés
       JSON respectant les contraintes (langues, format, nombre exact).
    3. Nettoie les résultats via `clean_keywords` et logge les avertissements
       si le nombre de mots-clés est insuffisant.
    4. Retourne une liste de dictionnaires contenant pour chaque requête :
       - "query" : la requête originale
       - "topic" : le sujet principal
       - "keywords" : la liste de mots-clés validés

    :param queries: Liste de chaînes représentant les catégories ou phrases à traiter.
    :type queries: list[str]

    :param model: Nom du modèle NLP utilisé pour générer les mots-clés.
    :type model: str

    :param n_keywords: Nombre exact de mots-clés à générer par sujet.
    :type n_keywords: int

    :return: Liste de dictionnaires avec la structure :
             [{"query": str, "topic": str, "keywords": list[dict]}, ...]
    :rtype: list[dict]

    :raises Exception: Toute erreur lors de l'appel au modèle NLP ou
                       du traitement JSON est loggée, et le processus
                       continue pour les autres requêtes.
    """
    results: list[dict] = []
    main_topics = extract_main_topic(queries, model)

    for query, main_topic in zip(queries, main_topics, strict=False):
        if not main_topic:
            continue

        logger.info(f"🎯 Sujet principal : {main_topic} (Objectif: {n_keywords})")

        prompt = f"""
        Génère une liste JSON de EXACTEMENT {n_keywords} mots-clés uniques pour le sujet "{main_topic}".

        Contraintes :
        - Format : [{{"keyword": "mot", "language": "fr"}}, {{"keyword": "word", "language": "en"}}]
        - Langues : un mélange de français et d'anglais.
        - Uniquement du JSON, pas de texte avant ou après.
        - Ne t'arrête pas avant d'avoir atteint {n_keywords} éléments.
        """

        try:
            response = ollama.chat(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                options={
                    "num_predict": 4096,  # Permet une réponse plus longue
                    "temperature": 0.8,  # Plus de diversité pour atteindre le nombre
                    "top_p": 0.9,
                },
            )

            content = response.get("message", {}).get("content", "").strip()

            # Nettoyage au cas où le modèle met des balises json
            if "```" in content:
                content = content.split("```")[1]
                if content.startswith("json"):
                    content = content[4:]
                content = content.strip()

            parsed = json.loads(content)

            # Nettoyage via votre utilitaire
            clean_list = clean_keywords(parsed)

            if len(clean_list) < n_keywords:
                logger.warning(
                    f"⚠️ Manque de mots-clés : {len(clean_list)}/{n_keywords} pour {main_topic}"
                )

            results.append(
                {
                    "query": query,
                    "topic": main_topic,
                    "keywords": clean_list,
                }
            )
            logger.info(f"✅ {len(clean_list)} mots-clés validés pour {main_topic}")

        except json.JSONDecodeError:
            logger.error(
                f"❌ Erreur de format JSON pour {main_topic}. Contenu reçu : {content[:100]}..."
            )
        except Exception as e:
            logger.error(f"❌ Erreur lors de la génération pour {main_topic} : {e}")

    return results
