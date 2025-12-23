import os

import psycopg2
from dotenv import load_dotenv
from psycopg2 import OperationalError

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


def create_tables():
    try:
        # Connexion à la base PostgreSQL
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        cur = conn.cursor()

        # Créer la schémas nommée "bluesky"
        cur.execute("CREATE SCHEMA IF NOT EXISTS bluesky;")
        cur.execute("SET search_path TO bluesky;")

        drop_tables_sql = """
        DROP TABLE IF EXISTS post_keywords CASCADE;
        DROP TABLE IF EXISTS posts CASCADE;
        DROP TABLE IF EXISTS keywords CASCADE;
        DROP TABLE IF EXISTS categories CASCADE;
        """
        cur.execute(drop_tables_sql)
        conn.commit()

        # Création des tables
        cur.execute("""
        CREATE TABLE IF NOT EXISTS categories (
            id SERIAL PRIMARY KEY,
            category TEXT NOT NULL UNIQUE
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS keywords (
            id SERIAL PRIMARY KEY,
            category_id INT REFERENCES categories(id),
            keyword TEXT NOT NULL,
            language TEXT NOT NULL,
            UNIQUE(category_id, keyword, language)
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS posts (
            id SERIAL PRIMARY KEY,
            external_id TEXT UNIQUE,
            content TEXT NOT NULL,
            language TEXT,
            created_at TIMESTAMP,
            like_count INT DEFAULT 0,
            reply_count INT DEFAULT 0,
            quote_count INT DEFAULT 0,
            repost_count INT DEFAULT 0,
            labels JSONB,
            embed JSONB,
            UNIQUE (external_id, content)  -- <-- no duplicate posts
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS post_keywords (
            post_id INT REFERENCES posts(id) ON DELETE CASCADE,
            keyword_id INT REFERENCES keywords(id) ON DELETE CASCADE,
            PRIMARY KEY(post_id, keyword_id)
        );
        """)

        conn.commit()

        # Vérification
        cur.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema = 'bluesky';
        """)
        tables = cur.fetchall()
        print("✅ Tables créées avec succès dans le schéma 'bluesky' !")
        print("📦 Liste des tables :", [t[1] for t in tables])

        cur.close()
        conn.close()

    except OperationalError as e:
        print("❌ Erreur de connexion :", e)
    except Exception as e:
        print("⚠️ Erreur :", e)


# Exécution
if __name__ == "__main__":
    create_tables()
