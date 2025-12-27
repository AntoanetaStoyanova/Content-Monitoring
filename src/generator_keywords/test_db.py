from src.db.postgresql_connector import get_connection


def test_db_count():
    print("🚀 Test de connexion en cours...")
    conn = None
    try:
        # 1. Récupération de la connexion
        conn = get_connection()

        # 2. Exécution de la requête
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM keywords;")
            count = cur.fetchone()[0]

            print("---")
            print("✅ CONNEXION RÉUSSIE !")
            print(f"📊 Nombre de mots-clés trouvés : {count}")
            print("---")

    except Exception as e:
        print(f"❌ ÉCHEC DU TEST : {e}")
    finally:
        # Toujours fermer la connexion
        if conn:
            conn.close()
            print("🔌 Connexion fermée.")


if __name__ == "__main__":
    test_db_count()
