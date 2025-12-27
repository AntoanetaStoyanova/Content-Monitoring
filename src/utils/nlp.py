import os
import sys

import spacy

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))


# Cache pour les modèles Spacy (évite de charger plusieurs fois)
_models = {"en": None, "fr": None}


def _get_model(lang: str):
    """Charge le modèle spacy seulement si nécessaire."""
    if lang == "en" and _models["en"] is None:
        _models["en"] = spacy.load("en_core_web_sm")
    elif lang == "fr" and _models["fr"] is None:
        _models["fr"] = spacy.load("fr_core_news_sm")
    return _models.get(lang)


def get_lemma(keyword: str, language: str) -> str:
    """Retourne la forme de base (lemme) du mot-clé."""
    lang = language.lower()
    nlp = _get_model(lang)

    if not nlp:
        return keyword

    doc = nlp(keyword)
    return doc[0].lemma_ if len(doc) > 0 else keyword


def generate_plural(keyword: str, language: str) -> str:
    """Génère un pluriel simple pour les mots anglais et français."""
    lang = language.lower()
    if lang == "en":
        if keyword.endswith(("s", "x", "z", "ch", "sh")):
            return keyword + "es"
        return keyword + "s"
    elif lang == "fr":
        if keyword.endswith(("s", "x", "z")):
            return keyword
        return keyword + "s"
    return keyword
