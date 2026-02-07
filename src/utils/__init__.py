# src/utils/__init__.py
"""Utilitaires pour tous les projets ML"""

# Config
from .config_models_train import (
    ALL_EMOJIS,
    BASE_PATH,
    DATA_PATH,
    EARLY_STOPPING_CONFIG,
    EMOJI_DICT,
    MAX_LENGTH,
    MODEL_CKPT,
    MODELS_ARTIFACTS_PATH,
    SEED,
    TRAINING_CONFIG,
)

# Model
from .model_utils import (
    create_datasets,
    load_model_and_encoder,
    make_predictions,
    plot_confusion_matrix,
    print_classification_report,
    save_model_and_encoder,
    set_seed,
    predict_emotion,
)

# Preprocessing
from .preprocessing import (
    load_and_preprocess_csv,
    preprocess_dataframe,
    preprocess_text_en,
)

# Trainer
from .trainer_utils import (
    WeightedTrainer,
    compute_metrics,
    get_class_weights,
    print_class_distribution,
)

# Emoji Augmentation ← NOUVEAU
from .emoji_augmentation import (
    add_emojis_to_dataset,
    get_augmentation_stats,
    print_augmentation_examples,
    augment_specific_classes,
    validate_augmentation,
)

__all__ = [
    # Config
    "BASE_PATH",
    "DATA_PATH",
    "MODELS_ARTIFACTS_PATH",
    "SEED",
    "MODEL_CKPT",
    "MAX_LENGTH",
    "ALL_EMOJIS",
    "EMOJI_DICT",
    "TRAINING_CONFIG",
    "EARLY_STOPPING_CONFIG",
    # Preprocessing
    "preprocess_text_en",
    "preprocess_dataframe",
    "load_and_preprocess_csv",
    # Trainer
    "WeightedTrainer",
    "compute_metrics",
    "get_class_weights",
    "print_class_distribution",
    # Model
    "set_seed",
    "save_model_and_encoder",
    "load_model_and_encoder",
    "create_datasets",
    "plot_confusion_matrix",
    "print_classification_report",
    "make_predictions",
    # Emoji Augmentation ← NOUVEAU
    "add_emojis_to_dataset",
    "get_augmentation_stats",
    "print_augmentation_examples",
    "augment_specific_classes",
    "validate_augmentation",
]