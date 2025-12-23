# Utilise une image Python légère comme base
FROM python:3.11-slim

# Définit le répertoire de travail
WORKDIR /app

# Copie le fichier de dépendances et les installe
COPY pyproject.toml uv.lock* ./
RUN pip install --no-cache-dir -r requirements.txt

# Copie le reste du code
COPY . .

# La commande finale sera remplacée par le "command" dans docker-compose
# ENTRYPOINT ["python", "votre_script_principal.py"]