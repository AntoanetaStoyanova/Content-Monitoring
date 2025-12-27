import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from collect.posts_db import save_posts_db

if __name__ == "__main__":
    USERNAME_BLUESKY = os.getenv("USERNAME_BLUESKY")
    PASSWORD_BLUESKY = os.getenv("PASSWORD_BLUESKY")
# get the keywords from the db , before or after collect the posts the make i v mn 4islo dumite, 
    save_posts_db(category=categories, n_keywords=50, n_post=100, generate_keywords=True)

