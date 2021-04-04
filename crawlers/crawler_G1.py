"""Author: Bruno Tatsuya Masunaga Santos
Organization: Universidade Federal do ABC (UFABC)
Project: COVID-19 Fake News Detection
Created in: 2021-04-03
Description: Crawler to get news from G1
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

ENDPOINT = 'https://g1.globo.com/busca/'
ORDER = 'recent'
SPECIES = 'notícias'

def get_html_news_G1(subject, date_from, time_from, date_to, time_to, page):
    """Sends GET HTTP request in order to get the HTML of G1 news query page
  
    Args:
        subject (string): Term used for querying news
        date_from (string): Initial date range for query (format: yyyy-MM-dd)
        time_from (string): Initial time range for query (format: hh:mm:ss)
        date_to (string): End date range for query (format: yyyy-MM-dd)
        time_to (string): End time range for query (format: hh:mm:ss)
        page (int): Number of page for query (1 to 40)

    Returns:
        string: HTML of G1 news query page | 'Failed to get HTML' if fails
    """

    uri = ENDPOINT                                          # set endpoint
    uri += '?q=' + subject                                  # set query subject
    uri += '&page=' + str(page)                              # set query page
    uri += '&order=' + ORDER                                 # set sorting rule
    uri += '&species=' + SPECIES                             # set query content type
    uri += '&from=' + date_from + 'T' + time_from + '-0300'  # set query init time range
    uri += '&to=' + date_to + 'T' + time_to + '-0300'        # set query end time range

    try:
        print('Getting URI: ' + uri + '\n')
        response = get(uri)
        print('Result GET: ' + str(response) + '\n')
        return response.text
    except:
        return 'Failed to get HTML'

def scrap_news_G1(html):
    """Gets headline, date and link of content of news contained in HTML
  
    Args:
        html (string): Open HTML of G1 news query page

    Returns:
        list of dictionary: Structured data of news scrapsed of HTML
    """

    news = []
    if not (html == 'Failed to get HTML'):
        page = BeautifulSoup(html, features='html.parser')
        news_containers = page.find_all('div', {'class': 'widget--info__text-container'})
        for nc in news_containers:
            # Get and treat news title
            if 'widget--info__title--ad' in str(nc):
                continue
            title = nc.find('div', {'class': 'widget--info__title product-color'}).text
            title = title.rstrip('\n').strip()
            # Get and treat news link
            link = nc.find('a')['href']
            link = get_between(link, 'https', 'ghtml', True)
            link = unquote(link)
            # Get and treat news date
            date_string = nc.find('div', {'class': 'widget--info__meta'}).text
            if "há" in date_string:
                int_value = int(get_between(date_string, ' ', ' '))
                if "hora" in date_string:
                    date = datetime.now() - timedelta(hours=int_value)
                if "dia" in date_string:
                    date = datetime.now() - timedelta(days=int_value)
            else:
                date = datetime.strptime(date_string, '%d/%m/%Y %Hh%M')
            # Construct object
            news_data = {'title': title, 'link': link, 'datetime': date}
            news.append(news_data)

    return news

if __name__ == '__main__':
    # Set target collection
    G1_COLLECTION = mongo_collection('raw_G1')

    # Get max datetime in collection (for duplicity control)
    try:
        MAX_DATETIME = G1_COLLECTION.find().sort([("datetime", -1)]).limit(1)[0]['datetime']
    except:
        MAX_DATETIME = datetime(1900,1,1,0,0,0)

    # Initializing datetime for querying 
    start_datetime = datetime.now()
    query_date = datetime(start_datetime.year, start_datetime.month, start_datetime.day)
    query_date_str = query_date.strftime("%Y-%m-%d")

    # Processing logic
    try:
        while (True):
            for page in range(1,41):
                html = get_html_news_G1('coronavirus', query_date_str, '00:00:00', 
                                        query_date_str, '23:59:59', page)
                news_scrapped = scrap_news_G1(html)
                if not news_scrapped:
                    break
                for news in news_scrapped:
                    # If news datetime is older than database registries, stop
                    if news['datetime'] < MAX_DATETIME:
                        raise NestedLoopBreaker()
                    inserted_id = insert_raw_collection(news, G1_COLLECTION)
                    print('Inserted object ' + inserted_id + ', dated on ' + 
                    news['datetime'].strftime("%Y-%m-%d") + 
                    'into raw_G1 collection')
            # Previous day
            query_date = query_date - timedelta(days=1)
            query_date_str = query_date.strftime("%Y-%m-%d")

    except NestedLoopBreaker:
        pass
    
    print('Execution finished')