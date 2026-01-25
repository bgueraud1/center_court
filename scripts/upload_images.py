# upload_images.py
import os
import cloudinary
import cloudinary.uploader
from pathlib import Path
import sys

# en tête du fichier upload_images.py
from pathlib import Path
from dotenv import load_dotenv

# charge .env depuis la racine du repo (parents[1] suppose scripts/ est dans la racine)
env_path = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(env_path)

# configure via env
cloudinary.config(
    cloud_name = os.environ.get("CLOUDINARY_CLOUD_NAME"),
    api_key = os.environ.get("CLOUDINARY_API_KEY"),
    api_secret = os.environ.get("CLOUDINARY_API_SECRET"),
    secure = True
)

IMAGES_DIR = Path("local_images")  # change this path si besoin

if not IMAGES_DIR.exists():
    print("Dossier local_images/ introuvable. Place tes images dans local_images/ et relance.")
    sys.exit(1)

for p in sorted(IMAGES_DIR.glob("*.*")):
    if p.is_file():
        name = p.stem  # ex: "331809-mirra-andreeva"
        public_id = f"players/{name}"
        print(f"Uploading {p.name} -> public_id={public_id} ...")
        try:
            res = cloudinary.uploader.upload(str(p),
                                             public_id=public_id,
                                             resource_type="image",
                                             overwrite=True,
                                             use_filename=False,
                                             unique_filename=False)
            print(" -> uploaded:", res.get("secure_url"))
        except Exception as e:
            print(" -> failed:", e)
