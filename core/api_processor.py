from core import mongodb_processor
from utils import constants
from twitterdev import recent_search
import json
import copy
import time
import traceback
import logging


class ApiProcessor:
    def __init__(self):
        self.next_token = ""
        self.mongo = mongodb_processor.MongoDBProcessor(constants.mongodb_url, constants.mongodb_db_name)
        pass

    def start_twitterdev(self):
        query_params = {"tweet.fields": constants.construct_tweet_fields_str(),
                        "max_results": 100,
                        "query": constants.construct_query_str()}

        if len(query_params["query"]) > 512:
            logging.debug("Invalid query. Length greater than 512")
            return ""

        json_data = copy.deepcopy(query_params)
        json_data["collection_start"] = f"start_{time.time()}"
        collection_name = json_data["collection_start"]
        self.save(f"query_{collection_name}", json_data)

        logging.debug(json_data)

        query_tuple_list = []
        for f, v in query_params.items():
            query_tuple_list.append((f, v))

        while True:
            if len(self.next_token) != 0:
                if len(query_tuple_list) == len(query_params) + 1:
                    query_tuple_list.pop()
                query_tuple_list.append(('next_token', self.next_token))

            try:
                json_response = recent_search.get(query_tuple_list=query_tuple_list)
            except Exception as e:
                traceback.logging.debug_exc()
                code, msg = e.args
                if code == 429:
                    logging.debug(f"Going to sleep at - {time.ctime()}")
                    time.sleep(15 * 60)
                    logging.debug(f"Awake at - {time.ctime()}")
                    continue
                else:
                    exit(0)

            if "meta" in json_response and "next_token" in json_response["meta"]:
                self.save(collection_name, json_response)
                self.next_token = json_response["meta"]["next_token"]
                collection_name = self.next_token
            else:
                break

    def save(self, collection, json_response):
        self.mongo.save(collection, json_response)
        # logging.debug(f"Saving to collection - {collection}")
        # logging.debug(json_response)
