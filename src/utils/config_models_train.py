# src/utils/config_models_train.py
# pour l'entraînement du modèle
"""Configuration centralisée pour tous les modèles"""
from pathlib import Path

# ============================================================
# PATHS - Adapté à ta structure
# ============================================================
BASE_PATH = Path(__file__).parent.parent.parent  # Remonte à content-monitoring/
DATA_PATH = BASE_PATH / "data" / "kaggle"
MODELS_ARTIFACTS_PATH = BASE_PATH / "src" / "model_train" / "artifacts"
NOTEBOOKS_PATH = BASE_PATH / "src" / "model_train"

# Créer les répertoires s'ils n'existent pas
MODELS_ARTIFACTS_PATH.mkdir(parents=True, exist_ok=True)

# ============================================================
# SEEDS & REPRODUCIBILITY
# ============================================================
SEED = 42

# ============================================================
# MODEL & TOKENIZER
# ============================================================
# ⚠️ CHANGEMENT: On utilise "roberta-base" directement dans le notebook
# Cette valeur est pour les anciens modèles fine-tunés
MODEL_CKPT = "roberta-base"  # ← CORRIGÉ: chaîne de caractères, pas un Path
MAX_LENGTH = 128

# ============================================================
# EMOJIS
# ============================================================
EMOJI_DICT = {
    "joy": ["😄", "😃", "😁", "😊", "😆", "🎉", "🥳"],
    "anger": ["😡", "😠", "🤬", "🔥", "💢"],
    "sad": ["😢", "😭", "😞", "😔", "😩", "😿"],
    "surprise": ["😲", "😮", "😯", "😳", "🫣"],
    "love": ["❤️", "😍", "🥰", "💖", "💕", "💞"],
    "fear": ["😨", "😱", "😰", "😖", "😧"],
    "neutral": ["😐", "😶", "🤐", "😑"],  # ← AJOUTÉ: emojis pour neutral
}

# ⚠️ IMPORTANT: Générer la liste de TOUS les emojis
ALL_EMOJIS = [e for sublist in EMOJI_DICT.values() for e in sublist]

# ============================================================
# TRAINING HYPERPARAMETERS
# ============================================================
TRAINING_CONFIG = {
    "num_train_epochs": 4,
    "per_device_train_batch_size": 8,
    "per_device_eval_batch_size": 8,
    "learning_rate": 5e-5,
    "weight_decay": 0.01,
    "warmup_steps": 500,
    "logging_steps": 100,
    "eval_strategy": "epoch",
    "save_strategy": "epoch",
    "load_best_model_at_end": True,
    "metric_for_best_model": "f1",
    "label_smoothing_factor": 0.1,
}

# ============================================================
# EARLY STOPPING
# ============================================================
EARLY_STOPPING_CONFIG = {
    "early_stopping_patience": 3,
    "early_stopping_threshold": 0.0,
}
