import datetime
import logging

from twitterdev import search_tweet_interface
from core import api_processor
from utils import constants, dbparser


def main():
    logging.basicConfig(filename='TwitterApp.log',
                        format='%(asctime)s::%(levelname)s: %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p',
                        level=logging.DEBUG)
    logging.debug("Starting Application...")
    logging.debug("Will be working with:")
    logging.debug("positive_hashtag = {}".format(constants.positive_hashtags))
    logging.debug(f"positive hash - {len(constants.positive_hashtags)}")
    logging.debug("negative_hashtag = {}".format(constants.negative_hashtags))
    logging.debug(f"negative hash - {len(constants.negative_hashtags)}")
    logging.debug("positive_keywords = {}".format(constants.positive_keywords))
    logging.debug(f"positive key- {len(constants.positive_keywords)}")
    logging.debug("negative_keywords = {}".format(constants.negative_keywords))
    logging.debug(f"negative key - {len(constants.negative_keywords)}")

    # apiProcessor = api_processor.ApiProcessor()
    # apiProcessor.start_twitterdev()

    full_archive_search = search_tweet_interface.FullArchiveSearch()
    full_archive_search.do_get()
    # full_archive_search.do_count()

    # dbparser.get_all_tweets()

if __name__ == '__main__':
    main()