import json
import logging
from datetime import datetime, date, timedelta
from core import mongodb_processor

from searchtweets import ResultStream, gen_request_parameters, load_credentials, collect_results
from utils import constants
import logging
import traceback


class FullArchiveSearch:
    def __init__(self):
        self.mongo = mongodb_processor.MongoDBProcessor(db_url=constants.mongodb_url, db_name=constants.mongodb_db_name)
        self.search_args = load_credentials("twitter_api_keys.yaml",
                                            yaml_key="search_tweets_v2",
                                            env_overwrite=False)

    def do_get(self):
        start_time = date.fromisoformat("2010-01-01")
        end_time = date.fromisoformat("2010-01-02")
        inc = timedelta(days=1)

        while start_time < end_time:
            collection_name = start_time.isoformat().replace("-", "")
            while True:
                try:
                    query_dict = constants.construct_query_param(start_time, inc)
                except ValueError as e:
                    logging.error(f"ValueError: {e}")
                    break

                logging.debug(f"query={query_dict}")

                rs = ResultStream(request_parameters=query_dict,
                                  max_results=query_dict["max_results"],
                                  max_pages=1,
                                  **self.search_args)

                logging.debug(f"ResultStream={rs}")

                # tweets = collect_results(query,
                #                          max_tweets=10,
                #                          result_stream_args=self.search_args)  # change this if you need to
                #
                # logging.debug(f"tweets={json.dumps(tweets)}")

                try:
                    tweets = list(rs.stream())
                    for page in tweets:
                        self.mongo.save(collection=collection_name, json_data=page)
                except:
                    traceback.print_stack()

            constants.reset_query_index()
            start_time = start_time + inc
