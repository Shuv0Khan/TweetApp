import json
from utils import constants
import tweepy
import tweepy_interface
from twitterdev import recent_search
from core import api_processor
import logging


def main():
    logging.basicConfig(filename='TwitterApp.log',
                        format='%(asctime)s::%(levelname)s: %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p',
                        level=logging.DEBUG)
    logging.debug("Starting Application...")
    logging.debug("Will be working with:")
    logging.debug("positive_hashtag = {}".format(constants.positive_hashtags))
    logging.debug("negative_hashtag = {}".format(constants.negative_hashtags))
    logging.debug("positive_keywords = {}".format(constants.positive_keywords))
    logging.debug("negative_keywords = {}".format(constants.negative_keywords))

    apiProcessor = api_processor.ApiProcessor()
    apiProcessor.start_twitterdev()


if __name__ == '__main__':
    main()
