"""Author: Bruno Tatsuya Masunaga Santos
Organization: Universidade Federal do ABC (UFABC)
Project: PGC-UFABC
Created in: 2021-04-03
Description: Crawler to get news from G1
"""

from bs4 import BeautifulSoup
from datetime import datetime
from pymongo import MongoClient
from requests import get
from urllib.parse import unquote
from crawlers_common import get_between
from crawlers_common import mongo_collection

ENDPOINT = 'https://g1.globo.com/busca/'
ORDER = 'recent'
SPECIES = 'notícias'
G1_COLLECTION = mongo_collection('raw_G1')

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
    uri += '&page' + str(page)                              # set query page
    uri += '&order' + ORDER                                 # set sorting rule
    uri += '&species' + SPECIES                             # set query content type
    uri += '&from' + date_from + 'T' + time_from + '-0300'  # set query init time range
    uri += '&to' + date_to + 'T' + time_to + '-0300'        # set query end time range

    try:
        response = get(uri)
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
        page = BeautifulSoup(html)
        news_containers = page.find_all('div', {'class': 'widget--info__text-container'})
        for nc in news_containers:
            # Get and treat news title
            title = nc.find('div', {'class': 'widget--info__title product-color'}).text
            title = title.rstrip('\n').strip()
            # Get and treat news link
            link = nc.find('a')['href']
            link = get_between(link, 'https', 'ghtml', True)
            link = unquote(link)
            # Get and treat news date
            date_string = nc.find('div', {'class': 'widget--info__meta'}).text
            if "há" in date_string:
                response = get(link)
                page_news = BeautifulSoup(response.text)
                date_string = page_news.find('time', {'itemprop': 'datePublished'}).text.strip()
                date = datetime.strptime(date_string, '%d/%m/%Y %Hh%M')
            # Construct object
            news_data = {'title': title, 'link': link, 'datetime': date}
            news.append(news_data)

    return news

def insert_raw_G1(data):
    """Inserts one object into raw_G1 MongoDB collection
  
    Args:
        data (dictionary): Data for inserting into raw_G1 collection
        (format: {'title': string, 'link': string, 'datetime': datetime})

    Returns:
        string: ID of inserted object
    """
    try: 
        inserted_id = G1_COLLECTION.insert_one(data).inserted_id
        return inserted_id
    except:
        return ''