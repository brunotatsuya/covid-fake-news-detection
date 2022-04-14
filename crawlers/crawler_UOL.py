"""Author: Bruno Tatsuya Masunaga Santos
Organization: Universidade Federal do ABC (UFABC)
Project: COVID-19 Fake News Detection
Created in: 2021-04-04
Description: Crawler to get news from UOL
"""

from bs4 import BeautifulSoup
from datetime import datetime
from pymongo import MongoClient
from requests import get
from crawlers_common import NestedLoopBreaker
from crawlers_common import get_between
from crawlers_common import mongo_collection
from crawlers_common import insert_raw_collection

ENDPOINT = 'https://noticias.uol.com.br/service/'
LOAD_COMPONENT = 'results-index'
CHUNK_SIZE = 50

def get_html_news_UOL(page):
    """Sends GET HTTP request in order to get the HTML of UOL news query page
  
    Args:
        page (int): Number of page for query

    Returns:
        string: HTML of UOL news query page | 'Failed to get HTML' if fails
    """
    paging = page*CHUNK_SIZE                            # set paging
    uri = ENDPOINT                                      # set endpoint
    uri += '?loadComponent=' + LOAD_COMPONENT           # set component to load
    uri += '&data=' + '''{"history":true,"dateFormat":"DD/MM/YYYY%20HH[h]mm", "busca":
                         {"params":{"size":''' + str(CHUNK_SIZE) + ''',"charset":"utf-8",
                          "repository":"mix2","sort":"created:desc","pgv3":true,
                          "tags-id":"72019","next":"0001H0U''' + str(paging) + 'N"}}}'

    try:
        print('Getting URI: ' + uri + '\n')
        response = get(uri)
        print('Result GET: ' + str(response) + '\n')
        return response.text
    except:
        return 'Failed to get HTML'

def scrap_news_UOL(html):
    """Gets headline, date and link of content of news contained in HTML
  
    Args:
        html (string): Open HTML of UOL news query page

    Returns:
        list of dictionary: Structured data of news scraped of HTML
    """

    news = []
    if not (html == 'Failed to get HTML'):
        page = BeautifulSoup(html, features='html.parser')
        news_containers = page.find_all('div', {'class': 'thumbnails-item grid col-xs-4 col-sm-6 small'})
        for nc in news_containers:
            # Get and treat news title
            title = nc.find('h3', {'class': 'thumb-title title-xsmall title-lg-small'}).text
            # Get and treat news link
            link = nc.find('a')['href']
            # Get and treat news date
            date_string = nc.find('time', {'class': 'thumb-date'}).text
            date = datetime.strptime(date_string, '%d/%m/%Y %Hh%M')
            # Construct object
            news_data = {'title': title, 'link': link, 'datetime': date}
            news.append(news_data)
    return news

if __name__ == '__main__':
    # Set target collection
    UOL_COLLECTION = mongo_collection('raw_UOL')

    # Get max datetime in collection (for duplicity control)
    try:
        MAX_DATETIME = UOL_COLLECTION.find().sort([("datetime", -1)]).limit(1)[0]['datetime']
    except:
        MAX_DATETIME = datetime(1900,1,1,0,0,0)

    # Processing logic
    page_number = 0
    try:
        while (True):
            html = get_html_news_UOL(page_number)
            news_scrapped = scrap_news_UOL(html)
            if not news_scrapped:
                break
            for news in news_scrapped:
                # If news datetime is older than database registries, stop
                if news['datetime'] < MAX_DATETIME:
                    raise NestedLoopBreaker()
                inserted_id = insert_raw_collection(news, UOL_COLLECTION)
                print('Inserted object ' + inserted_id + ', dated on ' + 
                news['datetime'].strftime("%Y-%m-%d") + ' into raw_UOL collection')
            page_number += 1

    except NestedLoopBreaker:
        pass
    
    print('Execution finished')