import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from db.posts_db import save_posts_db

if __name__ == "__main__":
    USERNAME_BLUESKY = os.getenv("USERNAME_BLUESKY")
    PASSWORD_BLUESKY = os.getenv("PASSWORD_BLUESKY")
    category = ["bulgaria"]
    save_posts_db(category=category, n_keywords=2, n_post=2, generate_keywords=True)
    # save_posts_db( n_keywords=3, n_post=3, generate_keywords=False)
