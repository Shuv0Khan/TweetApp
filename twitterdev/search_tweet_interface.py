import json
import logging
from datetime import datetime, date, timedelta
from core import mongodb_processor

from searchtweets import ResultStream, gen_request_parameters, load_credentials, collect_results, convert_utc_time
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
        start_time = date.fromisoformat("2006-04-01")
        end_time = date.fromisoformat("2020-12-31")
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

    def do_count(self):
        start_time = date.fromisoformat("2006-01-01")
        end_time = date.fromisoformat("2021-01-01")
        inc = timedelta(days=30)

        while start_time < end_time:
            collection_name = f"counts_{start_time.isoformat().replace('-', '')}"

            while True:
                try:
                    query_dict = constants.construct_query_param(start_time, inc)
                except ValueError as e:
                    logging.error(f"ValueError: {e}")
                    break

                query_dict["end_time"] = convert_utc_time(
                    start_time.replace(year=start_time.year + 1, month=1, day=1).isoformat())
                query_dict["granularity"] = "day"

                del query_dict["expansions"]
                del query_dict["tweet.fields"]
                del query_dict["user.fields"]
                del query_dict["place.fields"]
                del query_dict["max_results"]

                logging.debug(f"query={query_dict}")

                tweets = collect_results(query=query_dict,
                                         max_tweets=500,
                                         result_stream_args=self.search_args)

                logging.debug(f"tweets={json.dumps(tweets)}")
                for page in tweets:
                    self.mongo.save(collection=collection_name, json_data=page)

            constants.reset_query_index()
            start_time = start_time.replace(year=start_time.year + 1, month=1, day=1)
