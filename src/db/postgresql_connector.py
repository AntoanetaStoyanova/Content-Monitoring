import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()  # load environment variables from .env

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


def get_connection():
    """
    Établit une connexion à la base de données PostgreSQL.

    Cette fonction lit les informations de connexion depuis les variables
    d'environnement (`DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`),
    ouvre une connexion avec un timeout de 5 secondes, définit le
    `search_path` sur le schéma `bluesky` et retourne l'objet connexion.

    :return: Objet connexion PostgreSQL actif prêt à être utilisé.
    :rtype: psycopg2.extensions.connection

    :raises psycopg2.OperationalError: Si la connexion à la base échoue.
    """
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        connect_timeout=5,
    )
    with conn.cursor() as cur:
        cur.execute("SET search_path TO bluesky;")
    conn.commit()
    return conn
