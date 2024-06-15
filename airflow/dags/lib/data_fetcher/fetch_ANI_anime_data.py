import requests
import sys
import os
import json
import time
from datetime import date



query = '''
query ($page: Int, $perPage: Int, $sort: [MediaSort]) {
  Page(page: $page, perPage: $perPage) {
    media(sort: $sort, type: ANIME) {
      id
      title {
        romaji
        english
        native
      }
      averageScore
      genres
    }
  }
}
'''

def get_top_anime_statistics(page):
    variables = {
        'page': page,
        'perPage': 100,
        'sort': 'SCORE_DESC'
    }
    url = 'https://graphql.anilist.co'
    response = requests.post(url, json={'query': query, 'variables': variables})
    
    if response.status_code == 200:
        data = response.json()
        return data['data']['Page']['media']
    else:
        print(f"Request failed with status code: {response.status_code}")
        print(response.text)
        return None

def save_anime_statistics(anime_statistics):
    current_day = date.today().strftime("%Y%m%d")
    TARGET_PATH = os.path.join("datalake/raw/ANI/Top_anime", current_day)
    if not os.path.exists(TARGET_PATH):
        print("ANI creating folder", os.getcwd(), TARGET_PATH)
        os.makedirs(TARGET_PATH, exist_ok=True)
    print("save_anime_statistics cwd", os.getcwd())
    print("Writing here: ", TARGET_PATH)
    with open(os.path.join(TARGET_PATH, "ANI_top_anime.json"), "w", encoding='utf-8') as f:
        json.dump(anime_statistics, f, indent=4, ensure_ascii=False)

def format_anime_data(anime, ranking):
    return {
        "id": anime['id'],
        "title": {
            "romaji": anime['title']['romaji'],
            "english": anime['title']['english'],
            "native": anime['title']['native']
        },
        "averageScore": anime.get('averageScore', 'N/A'),
        "genres": anime['genres'],
        "ranking": ranking
    }

def main():
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
    save_anime_statistics(formatted_anime_statistics)


    for anime in formatted_anime_statistics:
        try:
            print(json.dumps(anime, ensure_ascii=False, indent=4))
        except UnicodeEncodeError:
            print(json.dumps(anime, ensure_ascii=False, indent=4).encode('ascii', 'replace').decode('ascii'))


  
