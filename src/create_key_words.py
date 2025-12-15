import json
import logging
import os
import re
import time
from typing import List

import ollama
import polars as pl
from beartype import beartype

# dossier de log
log_folder = os.path.join(os.getcwd(), "log")
os.makedirs(log_folder, exist_ok=True)
log_file = os.path.join(log_folder, "app.log")

logging.basicConfig(
    filename=log_file,
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

# partie qui remplace pour ce moment la base de données PostgreSQL
# dossier data
data_folder = os.path.join(os.getcwd(), "data")
os.makedirs(data_folder, exist_ok=True)
# csv pour les mots-clés
csv_path = os.path.join(data_folder, "keywords_generated.csv")

# à voir si on utilisera qu'un modèle
models = ["mistral:7b"]
# models = 'mistral:7b'


def clean_keywords(parsed: list) -> List[dict]:
    """
    Nettoie et déduplique une liste de mots-clés provenant de la réponse
    JSON d'un modèle NLP.

    Nettoyage effectué :
    - minuscules
    - suppression des espaces superflus
    - remplacement de "_" et "-" par des espaces
    - normalisation des espaces multiples
    - déduplication
    - filtrage des langues (fr, en)

    :param parsed: Liste d'objets JSON contenant 'keyword' et 'language'.
    :type parsed: list
    :returns: Liste nettoyée de mots-clés valides.
    :rtype: List[dict]

    :example:
    >>> data = [
    ...     {"keyword": " Politics ", "language": "en"},
    ...     {"keyword": "politics", "language": "en"},
    ...     {"keyword": "Gouvernement", "language": "fr"},
    ...     {"keyword": "", "language": "fr"},
    ...     {"keyword": "Science", "language": "de"}
    ... ]
    >>> clean_keywords(data)
    [{'keyword': 'politics', 'language': 'en'}, {'keyword': 'gouvernement', 'language': 'fr'}]
    """
    clean_list = []
    seen = set()

    for item in parsed:
        kw = item.get("keyword", "")
        lang = item.get("language", "").strip()

        # Normalisation du mot-clé
        kw = kw.lower()
        kw = kw.replace("_", " ").replace("-", " ")
        kw = re.sub(r"\s+", " ", kw).strip()

        if kw and lang in ("fr", "en") and kw not in seen:
            seen.add(kw)
            clean_list.append({"keyword": kw, "language": lang})

    return clean_list


@beartype
def extract_main_topic(query: str, model: str) -> str:
    prompt = f"""
    Tu es un modèle NLP professionnel.
    Résume le sujet principal de la phrase suivante en 1 à 3 mots seulement.
    Ne réponds pas avec un seul mot isolé si ce n'est pas suffisant pour le sens.
    Phrase : "{query}"
    Répond STRICTEMENT avec le ou les mots en minuscules.
    """
    response = ollama.chat(model=model, messages=[{"role": "user", "content": prompt}])
    raw = response["message"]["content"]
    topic = raw.strip().lower()
    return topic


# user can give a one word
@beartype
def generate_keywords(query: str, model: str, n_keywords: int = 10) -> List[dict]:
    """
    Génère une liste de mots-clés en anglais ou en français à partir d'une catégorie
    en utilisant un modèle NLP d'Ollama.

    La fonction envoie une requête au modèle pour générer exactement `n_keywords` mots-clés,
    respecte le format JSON attendu, et nettoie les résultats avec `clean_keywords()`.

    :param query: La catégorie à analyser pour générer des mots-clés.
    :type query: str
    :param model: Le nom du modèle NLP à utiliser pour générer les mots-clés.
    :type model: str
    :param n_keywords: Le nombre de mots-clés à générer. Par défaut 10.
    :type n_keywords: int, optional

    :returns: Une liste de mots-clés pertinents en anglais ou en français, sous forme de chaînes.
    :rtype: List[str]

    :raises ValueError: Si la réponse JSON du modèle est invalide.

    :log info:
        - Démarrage de la génération de mots-clés pour la requête.
        - Mots-clés générés après nettoyage.
    :log error:
        - Réponse JSON invalide reçue du modèle.

    :example:
        >>> generate_keywords("politic", "mistral:7b", 5)
        ["government", "politics", "republic", "diplomacy", "constitution"]
    """

    # 1️⃣ Extraire le sujet principal condensé
    main_topic = extract_main_topic(query, model)
    logging.info(f"Sujet principal extrait : {main_topic}")

    # 2️⃣ Générer plusieurs mots-clés autour de ce sujet
    prompt = f"""
    Tu es un modèle NLP professionnel.
    Génère EXACTEMENT {n_keywords} mots-clés pertinents autour du sujet : "{main_topic}".
    Répond STRICTEMENT en JSON sous la forme :
    [
        {{"keyword": "...", "language": "fr"}},
        {{"keyword": "...", "language": "en"}},
        ...
    ]
    Tous les mots-clés doivent être en minuscules et en anglais ou français.
    """
    response = ollama.chat(model=model, messages=[{"role": "user", "content": prompt}])
    raw = response["message"]["content"]
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        logging.error(f"❌ Réponse JSON invalide pour '{main_topic}': {raw}")
        raise ValueError(f"Réponse JSON invalide : {raw}")

    clean_list = clean_keywords(parsed)
    logging.info(f"✅ Mots-clés générés : {clean_list}")
    return clean_list


@beartype
def save_key_words_csv(category: str, n_keywords: int) -> None:
    """
    Génère des mots-clés pour une catégorie et les enregistre dans un fichier CSV.

    Cette fonction utilise les fonctions `generate_keywords()` et `clean_keywords()`,
    vérifie si le CSV existant des mots-clés existe, concatène les nouvelles données
    avec le CSV existant si nécessaire, et sauvegarde les résultats dans `key_words.csv`.
    Toutes les étapes sont loguées dans `log/app.log`. Si la catégorie existe déjà
    dans le CSV, aucune action n'est effectuée.

    :param category: Nom de la catégorie pour laquelle générer des mots-clés.
    :type category: str
    :param n_keywords: Nombre de mots-clés à générer pour cette catégorie.
    :type n_keywords: int

    :raises ValueError: Si la génération de mots-clés échoue ou si la réponse JSON du modèle est invalide.

    :log info: Informations sur le CSV existant ou la création d'un nouveau, démarrage et fin de la génération
               pour chaque modèle, confirmation de l'ajout de nouvelles catégories au CSV.
    :log error: Erreurs rencontrées lors de l'appel aux modèles.
    """

    # Créer le CSV vide si inexistant
    if not os.path.exists(csv_path):
        logging.info("📂 Aucun CSV existant trouvé. Création d'un nouveau.")
        df_existing = pl.DataFrame(schema=["query", "keyword", "language", "model"])
        df_existing.write_csv(csv_path)
    else:
        df_existing = pl.read_csv(csv_path)
        logging.info(f"📂 CSV existant trouvé avec {df_existing.height} lignes.")

    existing_queries = set(df_existing["query"].to_list())

    # Si la catégorie existe déjà, on ne fait rien
    if category in existing_queries:
        msg = f"✅ La catégorie '{category}' existe déjà dans le CSV. Rien à ajouter."
        print(msg)
        logging.info(msg)
        return

    # Générer les mots-clés
    all_rows = []
    for model in models:
        logging.info(f"🤖 Utilisation du modèle : {model}")
        try:
            results = generate_keywords(category, model, n_keywords=n_keywords)
            for item in results:
                all_rows.append(
                    {
                        "query": category,
                        "keyword": item["keyword"],
                        "language": item["language"],
                        "model": model,
                    }
                )
        except Exception as e:
            logging.error(f"⚠️ Problème avec le modèle {model} : {e}")
        time.sleep(1)

    # DataFrame pour les nouvelles données
    new_df = pl.DataFrame(all_rows)

    # Concaténation sécurisée
    if new_df.height > 0:
        df_to_save = pl.concat([df_existing, new_df])
        df_to_save.write_csv(csv_path)
        msg = f"✅ Nouvelle catégorie '{category}' ajoutée au CSV ({new_df.height} mots-clés)."
    else:
        msg = f"⚠️ Aucun mot-clé généré pour '{category}'. CSV non modifié."

    print(msg)
    logging.info(msg)
