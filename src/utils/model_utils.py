# src/utils/model_utils.py
# pour l'entraînement du modèle
"""Fonctions utilitaires pour modèles ML"""
import random
import numpy as np
import torch
import joblib
from pathlib import Path
from datasets import Dataset
from transformers import RobertaTokenizerFast
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import classification_report, confusion_matrix


def set_seed(seed: int = 42):
    """
    Fixe tous les seeds pour la reproducibilité
    
    Args:
        seed: Valeur du seed
    """
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.set_num_threads(8)


def save_model_and_encoder(model, tokenizer, label_encoder, save_path: str):
    """
    Sauvegarde le modèle, le tokenizer et l'encodeur de labels
    
    Args:
        model: Modèle transformers
        tokenizer: Tokenizer
        label_encoder: LabelEncoder
        save_path: Chemin de sauvegarde
    """
    path = Path(save_path)
    path.mkdir(parents=True, exist_ok=True)
    
    model.save_pretrained(path)
    tokenizer.save_pretrained(path)
    joblib.dump(label_encoder, path / "label_encoder.pkl")
    
    print(f"✅ Modèle sauvegardé dans: {path}")


def load_model_and_encoder(model_path: str, model_class):
    """
    Charge le modèle, le tokenizer et l'encodeur de labels
    
    Args:
        model_path: Chemin du modèle
        model_class: Classe du modèle (ex: RobertaForSequenceClassification)
    
    Returns:
        Tuple (model, tokenizer, label_encoder)
    """
    model_path = Path(model_path)
    
    model = model_class.from_pretrained(model_path)
    tokenizer = RobertaTokenizerFast.from_pretrained(model_path)
    label_encoder = joblib.load(model_path / "label_encoder.pkl")
    
    return model, tokenizer, label_encoder


def create_datasets(X: list, y: list, tokenizer, max_length: int = 128) -> Dataset:
    """
    Crée un dataset tokenisé
    
    Args:
        X: Textes
        y: Labels
        tokenizer: Tokenizer
        max_length: Longueur max
    
    Returns:
        Dataset tokenisé
    """
    def tokenize_fn(batch):
        return tokenizer(
            batch["text"], 
            truncation=True, 
            padding=True, 
            max_length=max_length
        )
    
    ds = Dataset.from_dict({"text": X, "label": y})
    return ds.map(tokenize_fn, batched=True).remove_columns(["text"])


def plot_confusion_matrix(y_true: list, y_pred: list, label_names: list, title: str = "Confusion Matrix", save_path: str = None):
    """
    Affiche et optionnellement sauvegarde une matrice de confusion
    
    Args:
        y_true: Labels vrais
        y_pred: Labels prédits
        label_names: Noms des classes
        title: Titre du graphique
        save_path: Chemin de sauvegarde (optionnel)
    """
    cm = confusion_matrix(y_true, y_pred)
    
    plt.figure(figsize=(12, 10))
    sns.heatmap(
        cm,
        annot=True,
        fmt="d",
        xticklabels=label_names,
        yticklabels=label_names,
        cmap="Blues"
    )
    
    plt.xlabel("Prédictions (Modèle)")
    plt.ylabel("Réalité (Labels)")
    plt.title(title)
    plt.tight_layout()
    
    if save_path:
        Path(save_path).parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(save_path)
        print(f"✅ Confusion matrix sauvegardée: {save_path}")
    
    plt.show()


def print_classification_report(y_true: list, y_pred: list, label_names: list):
    """
    Affiche un rapport de classification détaillé
    
    Args:
        y_true: Labels vrais
        y_pred: Labels prédits
        label_names: Noms des classes
    """
    print("\n" + "="*60)
    print("📊 CLASSIFICATION REPORT")
    print("="*60)
    print(classification_report(
        y_true,
        y_pred,
        target_names=label_names,
        digits=4
    ))


def make_predictions(texts: list, model, tokenizer, label_encoder, device="cpu", max_length: int = 128):
    """
    Fait des prédictions sur une liste de textes
    
    Args:
        texts: Liste de textes
        model: Modèle
        tokenizer: Tokenizer
        label_encoder: LabelEncoder
        device: Device (cpu ou cuda)
        max_length: Longueur max
    
    Returns:
        List de tuples (texte, émotion, confiance)
    """
    model.eval()
    results = []
    
    for text in texts:
        inputs = tokenizer(
            text,
            return_tensors="pt",
            truncation=True,
            padding=True,
            max_length=max_length
        ).to(device)
        
        with torch.no_grad():
            outputs = model(**inputs)
            logits = outputs.logits
            probabilities = torch.softmax(logits, dim=-1)
            prediction = torch.argmax(probabilities, dim=-1).item()
            confidence = probabilities[0][prediction].item()
            emotion = label_encoder.inverse_transform([prediction])[0]
        
        results.append((text, emotion, confidence))
    
    return results


def predict_emotion(
    text: str,
    model,
    tokenizer,
    label_encoder,
    threshold: float = 0.7,
    device: str = "cpu",
    strict: bool = False,
    temperature: float = 1.0,
):
    """
    Prédit l'émotion d'un texte avec seuil de confiance.

    Args:
        text: texte brut (déjà préprocessé)
        model: modèle HuggingFace
        tokenizer: tokenizer associé
        label_encoder: LabelEncoder
        threshold: seuil minimal de confiance
        device: "cpu" ou "cuda"
        strict: si True, ne retourne jamais 'neutral'
        temperature: temperature scaling (>=1.0)

    Returns:
        dict {
            "label": str,
            "confidence": float,
            "probs": dict
        }
    """
    model.eval()
    model.to(device)

    inputs = tokenizer(
        text,
        return_tensors="pt",
        truncation=True,
        padding=True,
        max_length=128,
    ).to(device)

    with torch.no_grad():
        logits = model(**inputs).logits
        logits = logits / temperature
        probs = torch.softmax(logits, dim=-1)[0].cpu().numpy()

    idx = int(np.argmax(probs))
    confidence = float(probs[idx])
    label = label_encoder.inverse_transform([idx])[0]

    probs_dict = {
        label_encoder.inverse_transform([i])[0]: float(p)
        for i, p in enumerate(probs)
    }

    # 🔒 Seuil de confiance
    if confidence < threshold:
        if strict:
            return {
                "label": "uncertain",
                "confidence": confidence,
                "probs": probs_dict,
            }
        else:
            return {
                "label": "neutral",
                "confidence": confidence,
                "probs": probs_dict,
            }

    return {
        "label": label,
        "confidence": confidence,
        "probs": probs_dict,
    }
