import logging
import os

# Dossier de log
log_folder = os.path.join(os.getcwd(), "log")
os.makedirs(log_folder, exist_ok=True)

# Fichier de log
log_file = os.path.join(log_folder, "app.log")

# Configuration du logging
logging.basicConfig(
    filename=log_file,
    filemode="a",  # 'a' pour append, 'w' pour overwrite
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

logger = logging.getLogger("bluesky_logger")
