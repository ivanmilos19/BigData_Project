import requests
import sys
import os
import json
import time
from datetime import date




def get_top_anime_statistics(page):
    url = f"https://api.jikan.moe/v4/top/anime?page={page}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data['data']
    except requests.exceptions.RequestException as e:
        print(f"Request failed for page {page}: {e}")
        return None
    except KeyError as e:
        print(f"Unexpected data structure for page {page}: {e}")
        return None

def save_anime_statistics(anime_statistics):
    current_day = date.today().strftime("%Y%m%d")
    TARGET_PATH = os.path.join("airflow/datalake/raw/MAL/Top_anime", current_day)
    if not os.path.exists(TARGET_PATH):
        os.makedirs(TARGET_PATH, exist_ok=True)
    print("Writing here: ", TARGET_PATH)
    with open(os.path.join(TARGET_PATH, "MAL_top_anime.json"), "w", encoding='utf-8') as f:
        json.dump(anime_statistics, f, indent=4, ensure_ascii=False)

def format_anime_data(anime, ranking):
    return {
        "id": anime['mal_id'],
        "title": {
            "romaji": anime['title'],
            "english": next((t['title'] for t in anime['titles'] if t['type'] == 'English'), None),
            "native": next((t['title'] for t in anime['titles'] if t['type'] == 'Japanese'), None)
        },
        "averageScore": anime.get('score', 'N/A'),
        "genres": [genre['name'] for genre in anime['genres']],
        "ranking": ranking
    }

anime_statistics = []
page = 1


while len(anime_statistics) < 300:
    page_data = get_top_anime_statistics(page)
    if page_data:
        anime_statistics.extend(page_data)
    else:
        break
    page += 1
    time.sleep(1)  


anime_statistics = anime_statistics[:300]


formatted_anime_statistics = [format_anime_data(anime, index + 1) for index, anime in enumerate(anime_statistics)]


for anime in formatted_anime_statistics:
    try:
        print(json.dumps(anime, ensure_ascii=False, indent=4))
    except UnicodeEncodeError:
        print(json.dumps(anime, ensure_ascii=False, indent=4).encode('ascii', 'replace').decode('ascii'))


save_anime_statistics(formatted_anime_statistics)
