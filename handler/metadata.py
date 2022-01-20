from pprint import pprint
# import sys
# from selenium import webdriver
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.common.by import By
from datetime import datetime
from actions_db.mongo_actions import MongoActions
from bs4 import BeautifulSoup as bs
import requests
import json
import re
from pymongo import UpdateOne
from common import config
import concurrent.futures
from handler.hash_unique import generate_hash_unique

class Games:

    """
    Clase encargada de scrapear toda la metadata de cada uno de los videojuegos.

    Parametros:
        - location: nos determina en que entorno vamos a trabajar (local/server)
    """

    def __init__(self, location):

        self.modeling_mongo = MongoActions(location)
        self.config = config()

        self.location = location

        self._scraping()

    def _scraping(self):

        """
        Metodo encargado de screaper toda la metadata de cada una de los videojuegos.

        """

        for_cookies = requests.get(self.config['sites']['web_videogames']['url_web'])
        cookies = for_cookies.cookies

        platforms = self.modeling_mongo._find_name_platform('Deeplink')

        contents = []
        score_votes_list = []

        for platform in platforms:
            print(platform)

            items = self.modeling_mongo._platform_find(platform, 'Deeplink')

            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                future_to_url = {
                    executor.submit(self._concurrent_scraping,item['Link'], cookies):
                        item for item in items
                }
                for future in concurrent.futures.as_completed(future_to_url):
                    data = future.result()

                    if data:
                        if data.get('aggregateRating'):
                            score_votes = data.get('aggregateRating')
                            score = score_votes.get('ratingValue') if score_votes.get('ratingValue') else None
                            votes = score_votes.get('ratingCount') if score_votes.get('ratingCount') else None
                        else:
                            score = None
                            votes = None

                        payload_contents = {
                            'UID': generate_hash_unique(data['name'],data['datePublished'][:4]),
                            'Title': data.get('name'),
                            'Genre': data.get('genre'),
                            'Description': data.get('description'),
                            'Year': data.get('datePublished')[:4],
                            'Image': data.get('image'),
                            'NumberOfPlayers':  data.get('numberOfPlayers') if data.get('numberOfPlayers') else None,
                            'Keywords': data.get('keywords').split('Ficha, ')[1] if data.get('keywords') else None
                        }
                        if not any(i['UID'] == payload_contents['UID'] for i in contents):
                            contents.append(payload_contents)

                        payload_votes_score = {
                            'UID': generate_hash_unique(data['name'],data['datePublished'][:4]),
                            'Title': data.get('name'),
                            'Score': score,
                            'Votes': votes,
                            'CreatedAt': datetime.now().strftime("%Y-%m-%d")
                        }
                        if not any(i['UID'] == payload_votes_score['UID'] for i in score_votes_list):
                            score_votes_list.append(payload_votes_score)

        #Insert Data
        pprint(contents)

        print('Inserting Content Metadata')
        data_content = []
        for content in contents:
            match = {
                "Title": content['Title']
            }
            data_content.append(
                UpdateOne(
                    match,
                    {'$set': content},
                    upsert=True
                )
            )
        self.modeling_mongo._insert_bulk(data_content,'Content')

        print('Inserting Score&Votes')
        self.modeling_mongo._add_data(score_votes_list,'ScoreVotes')
                                                
    def _concurrent_scraping(self, url, cookies):

        """
        Metodo encargado de extraer todo el contenido de cada una de las URLs.

        Parametros:
        - url: generados por el proceso de popularity
        - cookies: header del browser

        """

        headers = {"cookie": "txt_pais8=AR"}

        with requests.Session() as session:
            r = session.get(url, headers=headers, timeout=(5, 10), cookies=cookies)
            try:
                if r.status_code == 200:
                    html = r.content
                    try:
                        
                        html = bs(html.decode('UTF-8'), "html5lib")

                        json_react = html.find("script", string=re.compile(r"keywords"))
                        if json_react:
                            json_react = json_react.text
                            json_react = json_react.encode('UTF-8')
                            json_react = json.loads(json_react)

                        description = html.find("p", class_ = "s14 lh20 c2 mar_t15 mar_b10 fftit edit_desc").text
                        if description:
                            description: description.text

                        json_react['description'] = description

                        print(r.status_code, url)

                        return json_react
                        
                    except Exception as e:
                        print(f'Error {url}, {e}')
                else:
                    print(url, r.status_code)
            except requests.exceptions.Timeout as e:
                print(url, e)

        return False
