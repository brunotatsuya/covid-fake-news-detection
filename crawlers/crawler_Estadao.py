"""Author: Bruno Tatsuya Masunaga Santos
Organization: Universidade Federal do ABC (UFABC)
Project: COVID-19 Fake News Detection
Created in: 2021-04-04
Description: Crawler to get news from Estadao
"""

from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from pymongo import MongoClient
from requests import get
from urllib.parse import unquote
from crawlers_common import NestedLoopBreaker
from crawlers_common import get_between
from crawlers_common import mongo_collection
from crawlers_common import insert_raw_collection
from crawlers_common import portuguese_month_replacer

ENDPOINT = 'https://saude.estadao.com.br/modulos/ultimas'
MODULO = 'ultimas'
PRODUTO = 'Estadão'
EDITORIA = 'Saúde'
TIPO_MEDIA = 'Notícias'
ID_CANAL = 9
CHUNK_SIZE = 500
PATH_MODULO = 'portal/saude/modulos/'

def get_html_news_Estadao(page):
    """Sends GET HTTP request in order to get the HTML of Estadao news query page
  
    Args:
        page (int): Number of page for query - depends on CHUNK_SIZE

    Returns:
        string: HTML of Estadao news query page | 'Failed to get HTML' if fails
    """

    uri = ENDPOINT                                      # set endpoint
    uri += '?modulo=' + MODULO                          # set module name
    uri += '&config[busca][produto]=' + PRODUTO         # set prduct name
    uri += '&config[busca][editoria]=' + EDITORIA       # set category name
    uri += '&config[busca][tipo_midia]=' + TIPO_MEDIA   # set media type
    uri += '&config[busca][id_canal]=' + str(ID_CANAL)  # set channel id
    uri += '&config[busca][page]=' + str(page)          # set query page
    uri += '&config[busca][rows]='+ str(CHUNK_SIZE)     # set query chunk size
    uri += '&config[path_modulo]='+ str(PATH_MODULO)    # set module path

    try:
        print('Getting URI: ' + uri + '\n')
        response = get(uri)
        print('Result GET: ' + str(response) + '\n')
        return response.text
    except:
        return 'Failed to get HTML'

def scrap_news_Estadao(html):
    """Gets headline, date and link of content of news contained in HTML
  
    Args:
        html (string): Open HTML of Estadao news query page

    Returns:
        list of dictionary: Structured data of news scraped of HTML
    """

    news = []
    if not (html == 'Failed to get HTML'):
        page = BeautifulSoup(html, features='html.parser')
        news_containers = page.find_all('section', {'class': 'col-md-12 col-sm-12 col-xs-12 init item-lista'})
        for nc in news_containers:
            # Get and treat news date
            date_string = nc.find('span', {'class': 'data-posts'})
            if not date_string:
                continue
            date_string = date_string.text.strip()
            date_string = portuguese_month_replacer(date_string)
            date = datetime.strptime(date_string, '%d de %m de %Y | %Hh%M')
            # Get and treat news title
            obj_title = nc.find('a', {'class': 'link-title'})
            title = obj_title['title']
            # Get and treat news link
            link = obj_title['href']
            # Construct object
            news_data = {'title': title, 'link': link, 'datetime': date}
            news.append(news_data)
    return news

if __name__ == '__main__':
    # Set target collection
    ESTADAO_COLLECTION = mongo_collection('raw_Estadao')

    # Get max datetime in collection (for duplicity control)
    try:
        MAX_DATETIME = ESTADAO_COLLECTION.find().sort([("datetime", -1)]).limit(1)[0]['datetime']
    except:
        MAX_DATETIME = datetime(1900,1,1,0,0,0)

    # Processing logic
    page_number = 1
    try:
        while (True):
            html = get_html_news_Estadao(page_number)
            news_scrapped = scrap_news_Estadao(html)
            if not news_scrapped:
                break
            for news in news_scrapped:
                # If news datetime is older than database registries, stop
                if news['datetime'] < MAX_DATETIME:
                    raise NestedLoopBreaker()
                inserted_id = insert_raw_collection(news, ESTADAO_COLLECTION)
                print('Inserted object ' + inserted_id + ', dated on ' + 
                news['datetime'].strftime("%Y-%m-%d") + ' into raw_Estadao collection')
            page_number += 1

    except NestedLoopBreaker:
        pass
    
    print('Execution finished')