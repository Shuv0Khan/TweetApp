import requests
import os
import json
from utils import constants
import logging

bearer_token = constants.bearer_token
search_url = "https://api.twitter.com/2/tweets/search/recent"

# Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
# expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields

headersDic = {
    'Authorization': 'Bearer AAAAAAAAAAAAAAAAAAAAAA30TgEAAAAAdtqd1KsuXVp%2F%2BPMWDp0ju%2BiDXxc%3DyBFbdev3Kc505uj7EYSaaUQkH6m4uDcoy1jtjHqHaZLlI34DNP',
    'User-Agent': 'v2RecentSearchPython',
    'Cookie': 'guest_id=v1%3A163211856965853325; personalization_id="v1_gM2oFiFL2Mnbu8aglueTDA=="'
}


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2RecentSearchPython"
    return r


def connect_to_endpoint(url, params):
    # response = requests.get(url, headers=bearer_oauth(), params=params)
    response = requests.get(url,
                            headers=headersDic,
                            params=params)
    logging.debug(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


def get(query_tuple_list=[]):
    json_response = connect_to_endpoint(search_url, query_tuple_list)
    # logging.debug(json.dumps(json_response, indent=4, sort_keys=True))
    return json_response
