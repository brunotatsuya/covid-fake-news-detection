"""Author: Bruno Tatsuya Masunaga Santos
Organization: Universidade Federal do ABC (UFABC)
Project: COVID-19 Fake News Detection
Created in: 2021-04-04
Description: Crawler to get fake news from G1 Fato ou Fake
"""

from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from pymongo import MongoClient
from requests import get
from crawlers_common import NestedLoopBreaker
from crawlers_common import get_between
from crawlers_common import mongo_collection
from crawlers_common import insert_raw_collection

def get_html_fake_news_FatoFake(page):
    """Sends GET HTTP request in order to get the HTML of G1 Fato ou Fake news query page
  
    Args:
        page (int): Number of page for query

    Returns:
        string: HTML of G1 Fato ou Fake news query page | 'Failed to get HTML' if fails
    """

    uri = f'https://g1.globo.com/fato-ou-fake/coronavirus/index/feed/pagina-{page}.ghtml' # set endpoint                                     # set endpoint

    try:
        print('Getting URI: ' + uri + '\n')
        response = get(uri)
        print('Result GET: ' + str(response) + '\n')
        return response.text
    except:
        return 'Failed to get HTML'

def scrap_fake_news_FatoFake(html):
    """Gets headline, date and link of content of fake news contained in HTML
  
    Args:
        html (string): Open HTML of G1 Fato ou Fake news query page

    Returns:
        list of dictionary: Structured data of fake news scraped of HTML
    """

    fake_news = []
    if not (html == 'Failed to get HTML'):
        page = BeautifulSoup(html, features='html.parser')
        fake_news_containers = page.find_all('div', {'class': 'feed-post bstn-item-shape type-materia'})
        for nc in fake_news_containers:
            # Get and treat fake news title
            obj_title = nc.find('a', {'class': 'feed-post-link gui-color-primary gui-color-hover'})
            title = obj_title.text
            title = title.replace('É #FAKE que ', '')
            title = title.replace('É #FAKE ', '')
            # Get and treat fake_news link
            link = obj_title['href']
            # Get and treat fake_news date
            date_string = nc.find('span', {'class': 'feed-post-datetime'}).text
            date = datetime.now()
            if date_string == 'Ontem':
                date = date - timedelta(days=1)
            else:
                int_value = int(get_between(date_string, ' ', ' '))
                if "hora" in date_string:
                    date = date - timedelta(hours=int_value)
                if "dia" in date_string:
                    date = date - timedelta(days=int_value)
                if "semana" in date_string:
                    date = date - timedelta(weeks=int_value)
                if ("mes" in date_string) or ("mês" in date_string):
                    date = date - timedelta(days=int_value*30)
                if "ano" in date_string:
                    date = date - timedelta(days=int_value*365)
            # Construct object
            fake_news_data = {'title': title, 'link': link, 'datetime': date}
            fake_news.append(fake_news_data)
    return fake_news

if __name__ == '__main__':
    # Set target collection
    FATOFAKE_COLLECTION = mongo_collection('raw_FatoFake')

    # Get max datetime in collection (for duplicity control)
    try:
        MAX_DATETIME = FATOFAKE_COLLECTION.find().sort([("datetime", -1)]).limit(1)[0]['datetime']
    except:
        MAX_DATETIME = datetime(1900,1,1,0,0,0)

    # Processing logic
    page_number = 1
    try:
        while (True):
            html = get_html_fake_news_FatoFake(page_number)
            fake_news_scrapped = scrap_fake_news_FatoFake(html)
            for fake_news in fake_news_scrapped:
                # If fake news datetime is older than database registries, stop
                if fake_news['datetime'] < MAX_DATETIME:
                    raise NestedLoopBreaker()
                inserted_id = insert_raw_collection(fake_news, FATOFAKE_COLLECTION)
                print('Inserted object ' + inserted_id + ', dated on ' + 
                fake_news['datetime'].strftime("%Y-%m-%d") + ' into raw_FatoFake collection')
            page_number += 1

    except NestedLoopBreaker:
        pass
    
    print('Execution finished')
