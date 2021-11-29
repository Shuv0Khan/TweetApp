import copy
import json
import logging
import time
import traceback

from core import mongodb_processor
from twitterdev import recent_search
from utils import constants


class ApiProcessor:
    def __init__(self):
        self.next_token = ""
        self.mongo = mongodb_processor.MongoDBProcessor(constants.mongodb_url, constants.mongodb_db_name)
        pass

    def start_twitterdev(self):
        try:
            query_params = constants.construct_query_param()
        except ValueError:
            traceback.print_exc()
            return ""

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
                traceback.print_exc()
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

        self.start_twitterdev()

    def get_users(self):
        url = 'https://api.twitter.com/2/users'
        collection_name = 'users_info'

        file = open('UniqueUsers.log')
        lines = file.readlines()
        file.close()

        user_ids = []
        for line in lines:
            user_ids.append(line.strip())

        query_tuple_list = [('user.fields', 'created_at,description,entities,id,location,name,pinned_tweet_id,profile_image_url,protected,public_metrics,url,username,verified,withheld'),
                            ('expansions', 'pinned_tweet_id'),
                            ('tweet.fields', 'attachments,author_id,context_annotations,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,possibly_sensitive,public_metrics,referenced_tweets,reply_settings,source,text,withheld'),
                            ('ids', '')]

        id_index = 0
        while id_index < len(user_ids):
            ids = str(user_ids[id_index])
            id_index += 1
            for id in user_ids[id_index:id_index+99:1]:
                ids += ','+id
            id_index += 99

            query_tuple_list.pop()
            query_tuple_list.append(('ids', ids))

            while True:
                if len(self.next_token) != 0:
                    if len(query_tuple_list) > 4:
                        query_tuple_list.pop()
                    query_tuple_list.append(('next_token', self.next_token))

                try:
                    json_response = recent_search.connect_to_endpoint(url, query_tuple_list)
                    logging.debug(f"collected id index from {id_index-100} to {id_index}")
                except Exception as e:
                    traceback.print_exc()
                    code, msg = e.args
                    if code == 429:
                        logging.debug(f"Going to sleep at - {time.ctime()}")
                        time.sleep(5 * 60)
                        logging.debug(f"Awake at - {time.ctime()}")
                        continue
                    else:
                        exit(0)

                self.save(collection_name, json_response)

                if "meta" in json_response and "next_token" in json_response["meta"]:
                    self.next_token = json_response["meta"]["next_token"]
                else:
                    break

    def get_followers(self):
        url = 'https://api.twitter.com/1.1/followers/list.json'
        collection_name = 'followers'

        file = open('userset.csv', mode="r")
        lines = file.readlines()
        file.close()

        user_ids = []
        for line in lines:
            user_id = line.strip().strip(",")[0]
            user_ids.append(user_id)

        id_index = 0
        while id_index < len(user_ids):
            query_tuple_list = [('user_id', str(user_ids[id_index])),
                                ('count', '200'),
                                ('cursor', '-1')]
            all_followers = []

            while True:
                if len(self.next_token) != 0:
                    query_tuple_list.pop()
                    query_tuple_list.append(('cursor', self.next_token))

                try:
                    json_response = recent_search.connect_to_endpoint(url, query_tuple_list)
                    dict_response = json.loads(json_response)
                    all_followers.extend(dict_response['users'])
                    logging.debug(f"got response for userid: {user_ids[id_index]}, followers collected in this request: {len(dict_response['users'])}, total followers collected: {len(all_followers)}")
                except Exception as e:
                    traceback.print_exc()
                    code, msg = e.args
                    if code == 429:
                        logging.debug(f"Going to sleep at - {time.ctime()}")
                        time.sleep(15 * 60)
                        logging.debug(f"Awake at - {time.ctime()}")
                        continue
                    else:
                        exit(0)

                if "next_token" in json_response:
                    self.next_token = json_response["next_token"]
                else:
                    break

            followers_of_user = {}
            followers_of_user['user_id'] = user_ids[id_index]
            followers_of_user['followers'] = all_followers
            id_index += 1
            self.save(collection_name, followers_of_user)

    def save(self, collection, json_response):
        self.mongo.save(collection, json_response)
        logging.debug(f"Saving to collection - {collection}")
        # logging.debug(json_response)
