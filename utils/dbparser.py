import logging

from core import mongodb_processor
from utils import constants


def get_all_tweets():
    mongodb = mongodb_processor.MongoDBProcessor(constants.mongodb_url, constants.mongodb_db_name).my_db
    collections = mongodb.list_collection_names()
    for collection in collections:
        logging.debug("*****************************************************************************")
        logging.debug(f"******** Collection: {collection} *******************************************")
        logging.debug("*****************************************************************************")
        cursor = mongodb[collection].find({}, {"data.text": 1})

        for data_list in cursor:

            for tweet_list in data_list["data"]:
                logging.debug(tweet_list["text"])
