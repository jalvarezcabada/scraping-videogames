from os import PRIO_PGRP
from re import split
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from datetime import datetime
from actions_db.mongo_actions import MongoActions
from bs4 import BeautifulSoup
import requests
from pprint import pprint
from common import config
from datetime import datetime
from pymongo import UpdateOne


class TopPopularity:

    """
    Clase encargada de screapear el TOP 100 de juegos mas populares de cada una de las plataformas.

    Parametros:
        - location: nos determina en que entorno vamos a trabajar (local/server)
    """

    def __init__(self,type, location):
        self.modeling_mongo = MongoActions(location)

        self.config = config()
        self.type = type
        
        self.working()

    def working(self):

        """
        Metodo encargado de scrapear el HTML del sitio web, extraer los datos necesarios y almacenarlos en nuestra BD.
        """
        
        Session = requests.session()

        #opciones de navegacion
        #options = webdriver.ChromeOptions()
        #options.add_argument('--headless')
        #options.add_argument('--disable-extensions')
        #driver_path = self.config[type_scrap]['driver_mac']

        #driver  = webdriver.Chrome(driver_path, chrome_options = options)

        platform = self.config['sites']['web_videogames']['platforms_type']

        top_popularidad = []
        deeplinks = []
        updated_deeplinks = []

        for t in platform:

            response = Session.get(self.config['sites']['web_videogames'][self.type]['url_scrap'] + t)
            
            if response.status_code == 200:

                try:

                    soup = BeautifulSoup(response.text, 'html.parser')

                except Exception as e:
                    print(e)
                
                table = soup.find('div',class_='pad_rl8 fftext')

                for i in table.find_all('tr')[1:]:
                    link = i.find('a', class_='s14 cF fftit fw5').get('href')
                    platform = i.find('div', class_='mar_t1 a_c7 a_n').text
                    position = i.find('td', class_='pos_top ffnav').text
                    title = i.find('a', class_='s14 cF fftit fw5').text
                    genero = i.find('td', class_='tac dn600 wi74').text
                    year = i.find('td', class_='tac dn480 cF wi88').text
                    
                    if year != 'Por determinar':
                        year = year[-4:]
                    else:
                        year = None

                    payload_deeplinks = {
                        'Title':title,
                        'Platform': platform,
                        'Link': link
                    }

                    if not any(payload_deeplinks['Link'] == i['Link'] and payload_deeplinks['Platform'] == i['Platform'] for i in deeplinks):
                        deeplinks.append(payload_deeplinks)


                    payload_popularity = {
                        'Platform': platform,
                        'Position':position,
                        'Title':title,
                        'Genre':genero,
                        'Year':year,
                        'CreatedAt': datetime.now().strftime("%Y-%m-%d")
                    }
                    top_popularidad.append(payload_popularity)

            else: print('Page not found')
        
        pprint(top_popularidad)
                
        print(f'Inserting {len(top_popularidad)} in web_videogames...')
        self.modeling_mongo._add_data(top_popularidad, 'web_videogames')

        print('Inserting Deeplinks...')
        for data in deeplinks:
            match = {
                "Title": data['Title'],
                "Platform": data['Platform']
            }
            updated_deeplinks.append(
                UpdateOne(
                    match,
                    {'$set': data},
                    upsert=True
                )
            )
        if updated_deeplinks:
            self.modeling_mongo._insert_bulk(updated_deeplinks, 'Deeplink')
