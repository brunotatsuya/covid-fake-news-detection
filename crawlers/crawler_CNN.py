"""Author: Bruno Tatsuya Masunaga Santos
Organization: Universidade Federal do ABC (UFABC)
Project: COVID-19 Fake News Detection
Created in: 2021-04-03
Description: Crawler to get news from CNN
"""

import json
from datetime import datetime, timedelta
from pymongo import MongoClient
from requests import get
from urllib.parse import unquote
from crawlers_common import NestedLoopBreaker
from crawlers_common import get_between
from crawlers_common import mongo_collection
from crawlers_common import insert_raw_collection

ENDPOINT = 'https://api.cnnbrasil.com.br/110/v1/search/news'
CHUNK_SIZE = 500

def get_json_news_CNN(subject, page):
    """Sends GET HTTP request in order to get the JSON from CNN api news service
  
    Args:
        subject (string): Term used for querying news
        page (int): Number of page for query - depends on CHUNK_SIZE

    Returns:
        string: JSON string of CNN api news service | 'Failed to get JSON' if fails
    """

    uri = ENDPOINT                       # set endpoint
    uri += '?term=' + subject            # set query subject
    uri += '&limit=' + str(CHUNK_SIZE)   # set query chunk size
    uri += '&page=' + str(page)          # set query page

    try:
        print('Getting URI: ' + uri + '\n')
        response = get(uri)
        print('Result GET: ' + str(response) + '\n')
        return response.text
    except:
        return 'Failed to get JSON'

def scrap_news_CNN(json_string):
    """Gets headline, date and link of content of news contained in JSON
  
    Args:
        json_string (string): Open JSON string of CNN api news service

    Returns:
        list of dictionary: Structured data of news scraped of JSON
    """

    news = []
    if not (json_string == 'Failed to get JSON'):
        json_data = json.loads(json_string)
        news_dictionaries = list(json_data['result']['body']['Content']['List'])
        for nd in news_dictionaries:
            # Construct object
            news_data = {'title': nd['Title'], 'link': nd['canonical'], 
                         'datetime': datetime.fromtimestamp(nd['changedAt'])}
            news.append(news_data)
    return news

if __name__ == '__main__':
    # Set target collection
    CNN_COLLECTION = mongo_collection('raw_CNN')

    # Get max datetime in collection (for duplicity control)
    try:
        MAX_DATETIME = CNN_COLLECTION.find().sort([("datetime", -1)]).limit(1)[0]['datetime']
    except:
        MAX_DATETIME = datetime(1900,1,1,0,0,0)

    # Processing logic
    page_number = 1
    try:
        while (True):
            json_string = get_json_news_CNN('coronavirus', page_number)
            news_scrapped = scrap_news_CNN(json_string)
            if not news_scrapped:
                break
            for news in news_scrapped:
                # If news datetime is older than database registries, stop
                if news['datetime'] < MAX_DATETIME:
                    raise NestedLoopBreaker()
                inserted_id = insert_raw_collection(news, CNN_COLLECTION)
                print('Inserted object ' + inserted_id + ', dated on ' + 
                news['datetime'].strftime("%Y-%m-%d") + ' into raw_CNN collection')
            page_number += 1

    except NestedLoopBreaker:
        pass
    
    print('Execution finished')