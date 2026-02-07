# src/utils/trainer_utils.py
# pour l'entraînement du modèle
"""Trainer personnalisé et fonctions d'entraînement"""
import numpy as np
import torch
from torch.nn import CrossEntropyLoss
from transformers import Trainer
from sklearn.metrics import precision_recall_fscore_support, accuracy_score


# ============================================================
# WEIGHTED TRAINER
# ============================================================
class WeightedTrainer(Trainer):
    """
    Trainer personnalisé avec gestion des poids de classe.
    Utile pour les datasets déséquilibrés.
    
    Exemple:
        >>> class_weights = get_class_weights([0, 0, 0, 1, 2])
        >>> trainer = WeightedTrainer(
        ...     model=model,
        ...     args=training_args,
        ...     train_dataset=train_ds,
        ...     eval_dataset=val_ds,
        ...     class_weights=class_weights,
        ...     num_labels=3
        ... )
        >>> trainer.train()
    """
    def __init__(self, *args, class_weights=None, num_labels=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.class_weights = class_weights
        self.num_labels = num_labels
    
    def compute_loss(self, model, inputs, return_outputs=False, num_items_in_batch=None):
        """
        Calcule la loss avec poids de classe
        """
        labels = inputs.get("labels")
        outputs = model(**inputs)
        logits = outputs.get("logits")
        
        # Appliquer les poids si fournis
        if self.class_weights is not None:
            loss_fct = CrossEntropyLoss(
                weight=self.class_weights.to(model.device),
                label_smoothing=self.args.label_smoothing_factor
            )
        else:
            loss_fct = CrossEntropyLoss(
                label_smoothing=self.args.label_smoothing_factor
            )
        
        loss = loss_fct(logits.view(-1, self.num_labels), labels.view(-1))
        return (loss, outputs) if return_outputs else loss


# ============================================================
# COMPUTE METRICS
# ============================================================
def compute_metrics(pred, metric_name: str = 'macro'):
    """
    Calcule les métriques de classification
    
    Args:
        pred: Prédictions du trainer
        metric_name: Type de moyenne ('macro', 'weighted', 'micro')
            - 'macro': Traite chaque classe avec la même importance
            - 'weighted': Pondère par le nombre d'exemples par classe
            - 'micro': Moyenne globale
    
    Returns:
        Dict avec accuracy, f1, precision, recall
    
    Exemple:
        >>> metrics = compute_metrics(predictions, metric_name='macro')
        >>> print(metrics)
        {'accuracy': 0.93, 'f1': 0.92, 'precision': 0.91, 'recall': 0.90}
    """
    labels = pred.label_ids
    preds = np.argmax(pred.predictions, axis=1)
    
    precision, recall, f1, _ = precision_recall_fscore_support(
        labels, preds, average=metric_name, zero_division=0
    )
    acc = accuracy_score(labels, preds)
    
    return {
        'accuracy': acc,
        'f1': f1,
        'precision': precision,
        'recall': recall
    }


# ============================================================
# GET CLASS WEIGHTS
# ============================================================
def get_class_weights(y_train: list) -> torch.Tensor:
    """
    Calcule les poids des classes pour gérer le déséquilibre.
    
    Les classes rares reçoivent des poids plus élevés.
    Les classes communes reçoivent des poids plus faibles.
    
    Args:
        y_train: Liste des labels d'entraînement
    
    Returns:
        Tensor de poids normalisés
    
    Exemple:
        >>> y_train = [0, 0, 0, 0, 0, 1, 1, 1, 2, 2]  # 5 joy, 3 sad, 2 neutral
        >>> weights = get_class_weights(y_train)
        >>> print(weights)
        tensor([0.667, 1.000, 1.500])  # neutral a le plus grand poids
    """
    class_counts = np.bincount(y_train)
    
    # Poids inversement proportionnels à la fréquence
    weights = 1.0 / (class_counts + 1e-5)
    
    # Normaliser
    weights_tensor = torch.tensor(
        weights / weights.sum() * len(weights), 
        dtype=torch.float
    )
    
    return weights_tensor


# ============================================================
# PRINT CLASS DISTRIBUTION
# ============================================================
def print_class_distribution(y_train: list, label_names: list):
    """
    Affiche la distribution des classes
    
    Args:
        y_train: Liste des labels
        label_names: Noms des classes
    
    Exemple:
        >>> y_train = [0, 0, 0, 1, 1, 2]
        >>> label_names = ['joy', 'sad', 'neutral']
        >>> print_class_distribution(y_train, label_names)
        ⚖️ Class Distribution:
           joy: 3 (50.0%)
           sad: 2 (33.3%)
           neutral: 1 (16.7%)
    """
    class_counts = np.bincount(y_train)
    total = len(y_train)
    
    print("\n⚖️ Class Distribution:")
    for label, count in zip(label_names, class_counts):
        percentage = (count / total) * 100
        print(f"   {label}: {count} ({percentage:.1f}%)")
    print()