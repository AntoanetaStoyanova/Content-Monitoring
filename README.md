
# Générateur de Mots-Clés et Collecteur de Posts

Ce projet permet de générer automatiquement des mots-clés à partir de catégories et de collecter des posts sur Bluesky contenant ces mots-clés. Il est conçu pour les développeurs, data analysts ou toute personne souhaitant extraire des informations pertinentes sur des sujets spécifiques à partir de réseaux sociaux.


## Authors

- [@AntoanetaStoyanova](https://www.github.com/AntoanetaStoyanova)



## Features

1. Génération de mots-clés
    - Identifier automatiquement le sujet principal à partir d’une catégorie ou d’une phrase.
    - Générer des mots-clés pertinents en anglais ou en français via un modèle NLP (Ollama).
    Nettoyer et dédupliquer les mots-clés générés pour éviter les doublons.
    - Sauvegarder les mots-clés dans un CSV local (data/keywords_generated.csv).
    - Logger toutes les opérations (création, erreurs, succès) dans log/app.log.

2. Collecte de posts sur Bluesky
    - Se connecter à un compte Bluesky avec des identifiants stockés dans .env.
    - Rechercher des posts contenant les mots-clés générés.
    - Extraire les informations importantes de chaque post :
        - mot-clé utilisé
        - auteur du post
        - texte du post

    - Sauvegarder les posts dans un CSV local (data/collected_post.csv).
    - Possibilité de définir le nombre de mots-clés et de posts à récupérer pour chaque catégorie.
    - Utilisation de threads pour accélérer la collecte.

3. Fiabilité et suivi
    - Gestion des erreurs lors de la génération des mots-clés ou de la récupération des posts.

    - Logs détaillés pour suivre l’exécution du script.

## Deployment

1. Exécution locale

```bash
git clone <URL_DU_REPO>
cd <NOM_DU_REPO>
python3.13.7 -m venv venv
# Linux / Mac
source venv/bin/activate
# Windows
.venv\Scripts\activate
```
2. Installer les dépendances

```bash
pip install -r requirements.txt
```

4. Exécuter les scripts pour générer les mots-clés et collecter les posts

```bash
  python src/collect_posts.py
```


## Environment Variables

Créer un fichier .env à la racine du projet avec vos identifiants Bluesky

```
USERNAME_BLUESKY=<votre_username>
PASSWORD_BLUESKY=<votre_password>
```


## Documentation



