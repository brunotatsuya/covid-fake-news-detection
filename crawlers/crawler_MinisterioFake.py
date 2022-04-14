"""Author: Bruno Tatsuya Masunaga Santos
Organization: Universidade Federal do ABC (UFABC)
Project: COVID-19 Fake News Detection
Created in: 2021-04-04
Description: Crawler to get fake news from Ministerio da Saude
"""

from bs4 import BeautifulSoup
from datetime import datetime
from pymongo import MongoClient
from requests import get
from crawlers_common import NestedLoopBreaker
from crawlers_common import mongo_collection
from crawlers_common import insert_raw_collection

ENDPOINT = 'https://antigo.saude.gov.br/component/tags/tag/novo-coronavirus-fake-news'
CHUNK_SIZE = 20

def get_html_fake_news_Ministerio(page):
    """Sends GET HTTP request in order to get the HTML of Ministerio da Saude fake news query page
  
    Args:
        page (int): Number of page for query - depends on number of registries (20)

    Returns:
        string: HTML of Ministerio da Saude fake news query page | 'Failed to get HTML' if fails
    """
    paging = page*CHUNK_SIZE          # set paging
    uri = ENDPOINT                    # set endpoint
    uri += '?start=' + str(paging)    # set module name

    try: 
        print('Getting URI: ' + uri + '\n')
        response = get(uri)
        print('Result GET: ' + str(response) + '\n')
        return response.text
    except:
        return 'Failed to get HTML'

def scrap_fake_news_Ministerio(html):
    """Gets headline and link of content of fake news contained in HTML
  
    Args:
        html (string): Open HTML of Ministerio da Saude fake news query page

    Returns:
        list of dictionary: Structured data of fake news scraped of HTML
    """

    fake_news = []
    if not (html == 'Failed to get HTML'):
        page = BeautifulSoup(html, features='html.parser')
        fake_news_containers = page.find_all('td', {'class':'list-title'})
        for nc in fake_news_containers:
            # Get and treat fake news title
            obj_title = nc.find('a')
            title = obj_title.text.strip()
            title = title.replace(' - É FAKE NEWS!','')
            # Get and treat fake news link
            link = nc.find('a')['href']
            link = 'https://antigo.saude.gov.br' + link
            # Get and treat fake news date
            date = datetime.now()
            # Construct object
            fake_news_data = {'title': title, 'link': link, 'datetime': date}
            fake_news.append(fake_news_data)
    return fake_news

if __name__ == '__main__':
    # Set target collection
    MINISTERIOFAKE_COLLECTION = mongo_collection('raw_MinisterioFake')

    # Processing logic
    page_number = 0
    try:
        while (True):
            html = get_html_fake_news_Ministerio(page_number)
            fake_news_scrapped = scrap_fake_news_Ministerio(html)
            for fake_news in fake_news_scrapped:
                # If news alredy exists in database registries, stop
                if MINISTERIOFAKE_COLLECTION.count_documents({'title': fake_news['title']}):
                    raise NestedLoopBreaker()
                inserted_id = insert_raw_collection(fake_news, MINISTERIOFAKE_COLLECTION)
                print('Inserted object ' + inserted_id + ', dated on ' + 
                fake_news['datetime'].strftime("%Y-%m-%d") + ' into raw_MinisterioFake collection')
            page_number += 1

    except NestedLoopBreaker:
        pass
    
    print('Execution finished')
