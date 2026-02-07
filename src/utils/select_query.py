import os
import sys

# sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
import polars as pl

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.db.postgresql_connector import get_connection


def execute_query(query: str, return_df: bool = True):
    """
    Exécute une requête SQL sur la base de données et retourne le résultat.

    Args:
        query (str): La requête SQL à exécuter.
        return_df (bool): Si True, retourne un Polars DataFrame. Sinon retourne une liste de tuples.

    Returns:
        pl.DataFrame ou list: Résultat de la requête.
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(query)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
    except Exception as e:
        print("Erreur lors de l'exécution de la requête :", e)
        results, columns = [], []
    finally:
        cursor.close()
        conn.close()

    if return_df:
        if results:
            # Conversion en Polars DataFrame
            return pl.DataFrame(results, schema=columns, orient="row")
        else:
            return pl.DataFrame(schema=columns, orient="row")
    else:
        return results
