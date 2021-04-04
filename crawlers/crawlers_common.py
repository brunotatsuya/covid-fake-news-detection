"""Author: Bruno Tatsuya Masunaga Santos
Organization: Universidade Federal do ABC (UFABC)
Project: COVID-19 Fake News Detection
Created in: 2021-04-03
Description: Common functions to help crawlers development
"""

import re
from pymongo import MongoClient


class NestedLoopBreaker(Exception): pass

def get_between(base_string, before, after, includes = False):
    """Gets matched substring between substrings
  
    Args:
        base_string (string): Base substring for querying match
        before (string): Substring located BEFORE the desired substring
        after (string): Substring located AFTER the desired substring
        includes (bool): If True, result value will include the substrings before/after

    Returns:
        string: Substring found between the other substrings parametrized
    """

    matched = re.search(before + '(.*)' + after, base_string)
    if (matched):
        if (includes):
            return matched[0]
        return matched[1]
    else: 
        return ''

def mongo_collection(collection):
    """Creates new object to a specific MongoDB collection
  
    Args:
        collection (string): Name of target MongoDB collection

    Returns:
        pymongo.collection.Collection: MongoDB collection targered
    """
    client = MongoClient('localhost', 27017)
    db = client['covid-fake-news-detection']
    collection = db[collection]
    return collection


def insert_raw_collection(data, collection):
    """Inserts one object into MongoDB collection
  
    Args:
        data (dictionary): Data for inserting into collection
        collection (pymongo.collection.Collection): MongoDB collection targered

    Returns:
        string: ID of inserted object
    """
    try: 
        inserted_id = collection.insert_one(data).inserted_id
        return str(inserted_id)
    except:
        return ''

def portuguese_month_replacer(base_string):
    """Replaces Portuguese month name with its corresponding number into string
  
    Args:
        base_string (string): String containing Portuguese month name

    Returns:
        string: New string (lowered) with Portuguese month name replaced by its number
    """

    base_string = base_string.lower()
    mapping = {'janeiro': 1, 'fevereiro': 2, 'março': 3,
               'abril': 4, 'maio': 5, 'junho': 6,
               'julho': 7, 'agosto': 8, 'setembro': 9, 
               'outubro': 10, 'novembro': 11, 'dezembro': 12}
    for name, number in mapping.items():
        if name in base_string:
            base_string = base_string.replace(name, str(number))
            break
    return base_string