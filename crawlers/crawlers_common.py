"""Author: Bruno Tatsuya Masunaga Santos
Organization: Universidade Federal do ABC (UFABC)
Project: PGC-UFABC
Created in: 2021-04-03
Description: Common functions to help crawlers development
"""

import re
from pymongo import MongoClient

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
    if (includes):
        return matched[0]
    return [1]

def mongo_collection(collection):
    """Creates new object to a specific MongoDB collection
  
    Args:
        collection (string): Name of target MongoDB collection

    Returns:
        pymongo.collection.Collection: MongoDB collection targered
    """
    client = MongoClient('localhost', 27017)
    db = client['pgc_ufabc']
    collection = db[collection]
    return collection