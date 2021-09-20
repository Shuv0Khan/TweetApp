import logging
import time

import tweepy


class Interface:
    def __init__(self, api_key, secret_key, bearer_token):
        self.api_key = api_key
        self.secret_key = secret_key
        self.bearer_token = bearer_token

    def authorize(self):
        try:
            auth = tweepy.AppAuthHandler(self.api_key, self.secret_key)
            global api
            api = tweepy.API(auth)
        except:
            logging.debug("Something went wrong at authentication")

    def limit_handled(self, cursor):
        while True:
            try:
                yield cursor.next()
            except tweepy.RateLimitError:
                time.sleep(15 * 60)
        '''
            for follower in limit_handled(tweepy.Cursor(api.followers).items()):
                if follower.friends_count < 300:
                    logging.debug(follower.screen_name)
        '''

    def create_tweepy_client(self):
        self.tweepy_client = tweepy.Client()
