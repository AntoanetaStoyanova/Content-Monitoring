# src/utils/emoji_augmentation.py
"""
Module d'augmentation de données avec emojis pour la classification d'émotions.

Permet d'ajouter des emojis au dataset d'entraînement de manière contrôlée.
"""

import random

import numpy as np
import polars as pl


# ============================================================
# EMOJI AUGMENTATION
# ============================================================
def add_emojis_to_dataset(
    df: pl.DataFrame,
    emoji_dict: dict[str, list[str]],
    label_col: str = "label",
    text_col: str = "text_clean",
    augment_ratio: float = 0.5,
    seed: int = 42,
) -> pl.DataFrame:
    """
    Ajoute une colonne 'text_with_emoji' avec des emojis aléatoires.

    ⚠️ IMPORTANT:
    - Utilise une colonne SÉPARÉE (text_with_emoji) pour ne pas modifier l'original
    - Permet au modèle d'apprendre avec ET sans emojis
    - augment_ratio=0.5 → 50% avec emoji, 50% sans

    Args:
        df (pl.DataFrame): DataFrame Polars contenant les textes et labels
        emoji_dict (dict): Dictionnaire {emotion: [emoji1, emoji2, ...]}
                          Exemple: {"joy": ["😊", "😄"], "sad": ["😢", "😭"]}
        label_col (str): Nom de la colonne contenant les émotions (default: "label")
        text_col (str): Nom de la colonne contenant le texte (default: "text_clean")
        augment_ratio (float): Proportion de samples à augmenter (0.0 à 1.0)
                             - 0.0 = 0% avec emoji (pas d'augmentation)
                             - 0.5 = 50% avec emoji, 50% sans ← RECOMMANDÉ
                             - 1.0 = 100% avec emoji
        seed (int): Seed pour reproductibilité (default: 42)

    Returns:
        pl.DataFrame: DataFrame original + nouvelle colonne 'text_with_emoji'

    Exemple:
        >>> train_df = add_emojis_to_dataset(
        ...     train_df,
        ...     emoji_dict={"joy": ["😊", "😄"], "sad": ["😢"]},
        ...     augment_ratio=0.5,
        ...     seed=42
        ... )
        >>> train_df.columns
        ['text', 'label', 'text_clean', 'text_with_emoji']
    """
    # ✅ Fixer les seeds
    random.seed(seed)
    np.random.seed(seed)

    def augment_text(text: str, label: str, should_augment: bool) -> str:
        """
        Augmente un texte avec un emoji aléatoire selon l'émotion.

        Args:
            text: Texte à augmenter
            label: Label (émotion) du texte
            should_augment: Boolean indiquant si on doit ajouter un emoji

        Returns:
            Texte avec ou sans emoji ajouté
        """
        # Si pas d'augmentation OU label pas dans le dictionnaire → retourner le texte original  # noqa: E501
        if not should_augment or label not in emoji_dict:
            return text

        # Sélectionner un emoji aléatoire pour cette émotion
        emoji_choice = random.choice(emoji_dict[label])

        # Ajouter l'emoji à la fin du texte (séparé par un espace)
        return f"{text} {emoji_choice}"

    # ============================================================
    # Créer une liste booléenne aléatoire (qui reçoit emoji?)
    # ============================================================
    should_augment_list = [random.random() < augment_ratio for _ in range(len(df))]

    # ============================================================
    # Extraire les colonnes comme listes Python
    # ============================================================
    texts = df[text_col].to_list()
    labels = df[label_col].to_list()

    # ============================================================
    # Appliquer l'augmentation texte par texte
    # ============================================================
    text_with_emoji = [
        augment_text(texts[i], labels[i], should_augment_list[i])
        for i in range(len(df))
    ]

    # ============================================================
    # Retourner le DataFrame avec la nouvelle colonne
    # ============================================================
    return df.with_columns(pl.Series("text_with_emoji", text_with_emoji))


# ============================================================
# STATISTIQUES D'AUGMENTATION
# ============================================================
def get_augmentation_stats(
    df: pl.DataFrame,
    text_col: str = "text_clean",
    augmented_col: str = "text_with_emoji",
) -> dict:
    """
    Retourne des statistiques sur l'augmentation emoji.

    Args:
        df: DataFrame avec colonnes text_col et augmented_col
        text_col: Nom de la colonne texte original
        augmented_col: Nom de la colonne augmentée

    Returns:
        Dict avec statistiques

    Exemple:
        >>> stats = get_augmentation_stats(train_df)
        >>> print(f"Augmentés: {stats['augmented_count']} ({stats['augmented_ratio']:.1%})")
    """
    original = df[text_col].to_list()
    augmented = df[augmented_col].to_list()

    # Compter combien ont été augmentés (texte différent)
    augmented_count = sum(
        1 for o, a in zip(original, augmented, strict=False) if o != a
    )
    total = len(df)
    augmented_ratio = augmented_count / total if total > 0 else 0

    return {
        "total_samples": total,
        "augmented_count": augmented_count,
        "augmented_ratio": augmented_ratio,
        "non_augmented_count": total - augmented_count,
    }


# ============================================================
# AFFICHER LES EXEMPLES D'AUGMENTATION
# ============================================================
def print_augmentation_examples(
    df: pl.DataFrame,
    text_col: str = "text_clean",
    augmented_col: str = "text_with_emoji",
    label_col: str = "label",
    num_with_emoji: int = 2,
    num_without_emoji: int = 2,
) -> None:
    """
    Affiche des exemples d'augmentation emoji dans le dataset.

    Args:
        df: DataFrame avec colonnes à afficher
        text_col: Colonne texte original
        augmented_col: Colonne augmentée
        label_col: Colonne label
        num_with_emoji: Nombre d'exemples AVEC emoji à afficher
        num_without_emoji: Nombre d'exemples SANS emoji à afficher

    Exemple:
        >>> print_augmentation_examples(train_df, num_with_emoji=3, num_without_emoji=2)
    """
    print("\n" + "=" * 80)
    print("😊 EXEMPLES D'AUGMENTATION EMOJI")
    print("=" * 80)

    # Exemples AVEC emoji
    sample_with = df.filter(pl.col(augmented_col) != pl.col(text_col)).head(
        num_with_emoji
    )

    print(f"\n📝 Exemples AVEC emoji ({sample_with.height} affichés):")
    print("-" * 80)
    for i, row in enumerate(sample_with.iter_rows(named=True), 1):
        print(f"\n{i}. Label: {row[label_col].upper()}")
        print(f"   Original:      {row[text_col][:65]}...")
        print(f"   Avec emoji:    {row[augmented_col][:75]}...")

    # Exemples SANS emoji
    sample_without = df.filter(pl.col(augmented_col) == pl.col(text_col)).head(
        num_without_emoji
    )

    print(f"\n\n📝 Exemples SANS emoji ({sample_without.height} affichés):")
    print("-" * 80)
    for i, row in enumerate(sample_without.iter_rows(named=True), 1):
        print(f"\n{i}. Label: {row[label_col].upper()}")
        print(f"   Text: {row[augmented_col][:70]}...")

    print("\n" + "=" * 80)


# ============================================================
# AUGMENTATION PAR CLASSE
# ============================================================
def augment_specific_classes(
    df: pl.DataFrame,
    target_classes: list[str],
    emoji_dict: dict[str, list[str]],
    label_col: str = "label",
    text_col: str = "text_clean",
    augment_ratio: float = 1.0,
    seed: int = 42,
) -> pl.DataFrame:
    """
    Augmente SEULEMENT certaines classes (utile pour les classes rares).

    Args:
        df: DataFrame Polars
        target_classes: Liste des classes à augmenter (ex: ["surprise", "love"])
        emoji_dict: Dictionnaire d'emojis
        label_col: Colonne label
        text_col: Colonne texte
        augment_ratio: Ratio d'augmentation (1.0 = tous augmentés)
        seed: Seed pour reproductibilité

    Returns:
        DataFrame avec colonne 'text_with_emoji'

    Exemple:
        >>> # Augmenter SEULEMENT les classes rares
        >>> train_df = augment_specific_classes(
        ...     train_df,
        ...     target_classes=["surprise", "love"],
        ...     emoji_dict=EMOJI_DICT,
        ...     augment_ratio=1.0  # 100% pour les classes rares
        ... )
    """
    random.seed(seed)
    np.random.seed(seed)

    def augment_selective(text: str, label: str, should_augment: bool) -> str:
        if not should_augment or label not in emoji_dict:
            return text
        if label not in target_classes:
            return text
        emoji_choice = random.choice(emoji_dict[label])
        return f"{text} {emoji_choice}"

    # Créer les flags d'augmentation SEULEMENT pour les target_classes
    labels = df[label_col].to_list()
    should_augment_list = [
        random.random() < augment_ratio if label in target_classes else False
        for label in labels
    ]

    texts = df[text_col].to_list()

    text_with_emoji = [
        augment_selective(texts[i], labels[i], should_augment_list[i])
        for i in range(len(df))
    ]

    return df.with_columns(pl.Series("text_with_emoji", text_with_emoji))


# ============================================================
# VALIDATION: Vérifier que les emojis ne cassent rien
# ============================================================
def validate_augmentation(
    df_original: pl.DataFrame,
    df_augmented: pl.DataFrame,
    text_col: str = "text_clean",
    augmented_col: str = "text_with_emoji",
    label_col: str = "label",
) -> bool:
    """
    Valide que l'augmentation s'est bien passée.

    Vérifie que:
    - Même nombre de lignes
    - Même nombre de labels
    - Les textes augmentés sont différents des originaux (quand applicable)
    - Pas de valeurs NULL

    Args:
        df_original: DataFrame original
        df_augmented: DataFrame après augmentation
        text_col: Colonne texte
        augmented_col: Colonne augmentée
        label_col: Colonne label

    Returns:
        True si la validation passe, False sinon
    """
    checks = []

    # ✅ Check 1: Même nombre de lignes
    if len(df_original) != len(df_augmented):
        print("❌ Erreur: Nombre de lignes différent!")
        checks.append(False)
    else:
        print("✅ Check 1: Nombre de lignes identique")
        checks.append(True)

    # ✅ Check 2: Colonne ajoutée
    if augmented_col not in df_augmented.columns:
        print(f"❌ Erreur: Colonne '{augmented_col}' non trouvée!")
        checks.append(False)
    else:
        print(f"✅ Check 2: Colonne '{augmented_col}' présente")
        checks.append(True)

    # ✅ Check 3: Pas de NULL
    if df_augmented[augmented_col].is_null().any():
        print(f"❌ Erreur: Valeurs NULL trouvées dans '{augmented_col}'!")
        checks.append(False)
    else:
        print("✅ Check 3: Pas de valeurs NULL")
        checks.append(True)

    # ✅ Check 4: Labels identiques
    if df_original[label_col].to_list() != df_augmented[label_col].to_list():
        print("❌ Erreur: Labels différents!")
        checks.append(False)
    else:
        print("✅ Check 4: Labels identiques")
        checks.append(True)

    return all(checks)
