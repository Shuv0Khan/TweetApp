import json
from utils import constants
import tweepy
import tweepy_interface
from twitterdev import recent_search
from core import api_processor



def main():
    print("Starting Application...")
    print("Will be working with:")
    print("positive_hashtag = {}".format(constants.positive_hashtags))
    print("negative_hashtag = {}".format(constants.negative_hashtags))
    print("positive_keywords = {}".format(constants.positive_keywords))
    print("negative_keywords = {}".format(constants.negative_keywords))

    apiProcessor = api_processor.ApiProcessor()
    apiProcessor.start_twitterdev()

if __name__ == '__main__':
    main()
