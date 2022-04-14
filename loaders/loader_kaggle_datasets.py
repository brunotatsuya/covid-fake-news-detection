"""Author: Bruno Tatsuya Masunaga Santos
Organization: Universidade Federal do ABC (UFABC)
Project: COVID-19 Fake News Detection
Created in: 2021-04-04
Description: Loader for consuming Kaggle datasets
"""

import pandas as pd
import os
from pymongo import MongoClient

def load_dataset(dataset, name_dataset, title_str, label_str):
    """Inserts all registries of loaded dataset into MongoDB Collection
  
    Args:
        dataset (pandas.core.frame.DataFrame): Dataset to load
        name_dataset (string): Name string of dataset
        title_str (string): String for identifying the title of registry in dataset
        label_str (string): String for identifying the label of registry in dataset

    Returns:
        Nothing
    """

    for index, row in dataset.iterrows():
        data = {'title': row[title_str], 'label': row[label_str], 'origin': name_dataset}
        inserted_id = str(KAGGLE_COLLECTION.insert_one(data).inserted_id)
        print('Inserted object ' + inserted_id + f', of {name_dataset} dataset into raw_Kaggle collection')
    print(f'Finished loading of {name_dataset} dataset into raw_Kaggle collection')


if __name__ == '__main__':

    client = MongoClient('localhost', 27017)
    db = client['covid-fake-news-detection']
    KAGGLE_COLLECTION = db['raw_Kaggle']

    path = str(os.path.dirname(os.path.dirname(__file__)))

    # START LOADING AGHAMMADZADA DATASET
    aghammadzada1 = pd.read_csv(path + r'\datasets\Kaggle Aghammadzada\Constraint_Train.csv')
    aghammadzada2 = pd.read_csv(path + r'\datasets\Kaggle Aghammadzada\Constraint_Val.csv')
    aghammadzada = pd.concat([aghammadzada1, aghammadzada2])
    del aghammadzada1
    del aghammadzada2
    load_dataset(aghammadzada, 'Aghammadzada', 'tweet', 'label')
    del aghammadzada
    # END LOADING AGHAMMADZADA DATASET

    # START LOADING ARASHNIC DATASET
    arashnic_claimfake_1 = pd.read_csv(path + r'\datasets\Kaggle Arashnic\ClaimFakeCOVID-19_5.csv')
    arashnic_claimfake_2 = pd.read_csv(path + r'\datasets\Kaggle Arashnic\ClaimFakeCOVID-19_7.csv')
    arashnic_claimreal_1 = pd.read_csv(path + r'\datasets\Kaggle Arashnic\ClaimRealCOVID-19.csv')
    arashnic_claimreal_2 = pd.read_csv(path + r'\datasets\Kaggle Arashnic\ClaimRealCOVID-19_5.csv')
    arashnic_claimreal_3 = pd.read_csv(path + r'\datasets\Kaggle Arashnic\ClaimRealCOVID-19_7.csv')
    arashnic_newsfake_1 = pd.read_csv(path + r'\datasets\Kaggle Arashnic\NewsFakeCOVID-19.csv')
    arashnic_newsfake_2 = pd.read_csv(path + r'\datasets\Kaggle Arashnic\NewsFakeCOVID-19_5.csv')
    arashnic_newsfake_3 = pd.read_csv(path + r'\datasets\Kaggle Arashnic\NewsFakeCOVID-19_7.csv')
    arashnic_newsreal_1 = pd.read_csv(path + r'\datasets\Kaggle Arashnic\NewsRealCOVID-19.csv')
    arashnic_newsreal_2 = pd.read_csv(path + r'\datasets\Kaggle Arashnic\NewsRealCOVID-19_5.csv')
    arashnic_newsreal_3 = pd.read_csv(path + r'\datasets\Kaggle Arashnic\NewsRealCOVID-19_7.csv')
    arashnic_claimfake = pd.concat([arashnic_claimfake_1, arashnic_claimfake_2])
    arashnic_claimreal = pd.concat([arashnic_claimreal_1, arashnic_claimreal_2, arashnic_claimreal_3])
    arashnic_newsfake = pd.concat([arashnic_newsfake_1, arashnic_newsfake_2, arashnic_newsfake_3])
    arashnic_newsreal = pd.concat([arashnic_newsreal_1, arashnic_newsreal_2, arashnic_newsreal_3])
    del arashnic_claimfake_1
    del arashnic_claimfake_2
    del arashnic_claimreal_1
    del arashnic_claimreal_2
    del arashnic_claimreal_3
    del arashnic_newsfake_1
    del arashnic_newsfake_2
    del arashnic_newsfake_3
    del arashnic_newsreal_1
    del arashnic_newsreal_2
    del arashnic_newsreal_3
    arashnic_claimfake['label'] = 'fake'
    arashnic_claimreal['label'] = 'real'
    arashnic_newsfake['label'] = 'fake'
    arashnic_newsreal['label'] = 'real'
    arashnic = pd.concat([arashnic_claimfake, arashnic_claimreal, arashnic_newsfake, arashnic_newsreal])
    arashnic = arashnic[['title','label']]
    load_dataset(arashnic, 'Arashnic', 'title', 'label')
    del arashnic
    # END LOADING ARASHNIC DATASET
    
    # START LOADING BANIK DATASET
    banik = pd.read_csv(path + r'\datasets\Kaggle Banik\COVID Fake News Data.csv')
    banik['outcome'] = banik['outcome'].map(lambda l: 'fake' if not l else 'real')
    load_dataset(banik, 'Banik', 'headlines', 'outcome')
    del banik
    # END LOADING BANIK DATASET