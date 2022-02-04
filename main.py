import logging
import time
import traceback

from utils import dbparser
from twitterdev import search_tweet_interface
from twitterdev import api_processor
from utils import constants
import smtplib, ssl


def main(choice):
    if choice is None:
        print("Please provide the appropriate choice of operation.")
        return

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

    logging.debug(f"\n***************************\nSelected Operation: {choice}\n***************************\n")

    if choice == 'test':
        logging.debug("Choice captured successfully")

    elif choice == 'test_email':
        raise Exception("Testing email")

    elif choice.startswith('api'):
        api = api_processor.ApiProcessor()

        if choice == 'api_get_followers':
            api.get_followers()
        elif choice == 'api_get_followings':
            api.get_followings()
        elif choice == 'api_get_through_relay':
            api.get_followings_through_relay()
        elif choice == 'api_twitterdev':
            api.start_twitterdev()

    elif choice.startswith('full'):
        full_archive_search = search_tweet_interface.FullArchiveSearch()
        if choice == 'full_get':
            full_archive_search.do_get()
        elif choice == 'full_count':
            full_archive_search.do_count()

    elif choice.startswith('db'):
        if choice == 'db_all_tweets':
            dbparser.get_all_tweets()


if __name__ == '__main__':
    from sys import argv

    choice = None
    if len(argv) == 2:
        choice = argv[1]

    try:
        main(choice)
    except Exception as e:
        traceback.print_exc()
        logging.error(e)

        # Send email
        port = 465  # For SSL
        password = "Confirm@2022"

        # Create a secure SSL context
        context = ssl.create_default_context()

        with smtplib.SMTP_SSL("smtp.gmail.com", port, context=context) as server:
            server.login("tweetapp71@gmail.com", password)
            message = f"Subject: Collection Shutdown - {time.ctime()} \n\nShutdown with exception - {e}"
            server.sendmail("tweetapp71@gmail.com", "naweemshuvo@gmail.com", message)
        # exit
        exit(1)
