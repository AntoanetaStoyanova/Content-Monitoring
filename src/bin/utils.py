import csv
import os
import re
from collections.abc import Iterable

LANGUAGES_ALLOWED = {"fr", "en"}
SPACE_RE = re.compile(r"\s+")


def normalize_keyword(keyword: str) -> str:
    """
    Normalise un mot-clé :
    - minuscules
    - "_" et "-" remplacés par des espaces
    - espaces multiples normalisés
    """
    keyword = keyword.lower().replace("_", " ").replace("-", " ")
    return SPACE_RE.sub(" ", keyword).strip()


def clean_keywords(parsed: Iterable[dict]) -> list[dict]:
    """
    Nettoie et déduplique une liste de mots-clés issus d'un modèle NLP.

    :param parsed: Iterable d'objets contenant 'keyword' et 'language'
    :return: Liste nettoyée et filtrée
    """
    if not isinstance(parsed, Iterable):
        return []

    seen: set[tuple[str, str]] = set()
    result: list[dict] = []

    for item in parsed:
        if not isinstance(item, dict):
            continue

        keyword = normalize_keyword(str(item.get("keyword", "")))
        language = str(item.get("language", "")).strip().lower()

        if not keyword or language not in LANGUAGES_ALLOWED:
            continue

        key = (keyword, language)
        if key in seen:
            continue

        seen.add(key)
        result.append({"keyword": keyword, "language": language})

    return result


def get_project_root():
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

def write_posts_to_csv(posts: list[dict], filename: str):
    if not posts:
        return

    data_dir = os.path.join(get_project_root(), "data", "posts")
    os.makedirs(data_dir, exist_ok=True)

    file_path = os.path.join(data_dir, filename)

    # Lire les posts existants pour éviter les doublons
    existing_ids = set()
    if os.path.exists(file_path):
        with open(file_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_ids.add(row["external_id"])

    # Filtrer uniquement les nouveaux posts
    new_posts = [p for p in posts if p["external_id"] not in existing_ids]

    if not new_posts:
        print(f"[INFO] No new posts to append to {file_path}")
        return

    write_header = not os.path.exists(file_path)
    with open(file_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=new_posts[0].keys())
        if write_header:
            writer.writeheader()
        writer.writerows(new_posts)

    print(f"[INFO] CSV updated: {file_path} (+{len(new_posts)} posts)")
