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
