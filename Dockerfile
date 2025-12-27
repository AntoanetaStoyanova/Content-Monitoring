# Utilisation de Python 3.13-slim pour la légèreté
FROM python:3.13-slim

# Installation de uv depuis l'image officielle
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Environment variables
# 1. Prevent .pyc files
# 2. Real-time logging
# 3. Set project root in path
# 4. Use system environment for uv in container
# Variables d'environnement pour Python et uv
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    # Installe les packages globalement dans le Python du conteneur
    UV_PROJECT_ENVIRONMENT=/usr/local

WORKDIR /app

# Dépendances système pour psycopg2 (connecteur PostgreSQL)
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 1. Copier uniquement les fichiers de dépendances (optimise le cache)
COPY pyproject.toml uv.lock* ./

# 2. Installer les dépendances sans installer le projet lui-même
RUN uv sync --no-install-project --no-dev

# 3. Copier le reste de votre code source
COPY . .

# 4. Synchroniser le projet final
RUN uv sync --no-dev

# Par défaut, on lance le générateur de mots-clés
CMD ["python", "-m", "src.collect_posts.main"]
# "src.generator.main"]