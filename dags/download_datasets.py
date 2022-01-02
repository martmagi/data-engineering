import json
import requests

from pathlib import Path

RAW_DATA_URL = 'https://owncloud.ut.ee/owncloud/index.php/s/g4qB5DZrFEz2XLm/download/kym.json'
VISION_URL = 'https://owncloud.ut.ee/owncloud/index.php/s/teoFdWKBzzqcFjY/download/kym_vision.json'


def get_raw_dataset(kym_file_path: str):
    raw_file = Path(f"{kym_file_path}")
    if not raw_file.is_file():  # skip downloading if it already exists
        memes_raw_data = requests.get(RAW_DATA_URL).json()
        with open(f"{kym_file_path}", 'w') as f:
            json.dump(memes_raw_data, f, ensure_ascii=False)


def get_vision_dataset(vision_file_path: str):
    vision_file = Path(f"{vision_file_path}")
    if not vision_file.is_file():  # skip downloading if it already exists
        vision_data = requests.get(VISION_URL).json()
        with open(f"{vision_file_path}", 'w') as f:
            json.dump(vision_data, f, ensure_ascii=False)
