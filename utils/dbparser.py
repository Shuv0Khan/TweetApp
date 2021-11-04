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

def get_all_unique_author_ids():
    mongodb = mongodb_processor.MongoDBProcessor(constants.mongodb_url, constants.mongodb_db_name).my_db
    collections = mongodb.list_collection_names()

    users = {}
    for collection in collections:
        cursor = mongodb[collection].find({}, {"data.author_id": 1})

        for data_list in cursor:
            for tweet_list in data_list["data"]:
                author = tweet_list["author_id"]
                if author not in users.keys():
                    users[author] = 1

    for key in users:
        logging.debug(key)


def main():
    logging.basicConfig(filename='DBParser.log',
                        format='%(message)s',
                        level=logging.DEBUG)

    get_all_unique_author_ids()

if __name__ == '__main__':
    main()