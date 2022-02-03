import logging
import time
import traceback

from twitterdev import api_processor
from utils import constants
import smtplib, ssl


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

    # raise Exception("Testing email")

    apiProcessor = api_processor.ApiProcessor()
    # apiProcessor.get_followers()
    apiProcessor.get_followings()
    # apiProcessor.start_twitterdev()

    # full_archive_search = search_tweet_interface.FullArchiveSearch()
    # full_archive_search.do_get()
    # full_archive_search.do_count()

    # dbparser.get_all_tweets()


if __name__ == '__main__':
    try:
        main()
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
