import logging

import requests

from utils import constants

bearer_token = constants.bearer_token
search_url = "https://api.twitter.com/2/tweets/search/recent"

# Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
# expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields

headersDic = {
    'Authorization': f'Bearer {constants.bearer_token}',
    'User-Agent': 'v2UserLookupPython'
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


def get(url=search_url, query_tuple_list=[]):
    json_response = connect_to_endpoint(url, query_tuple_list)
    # logging.debug(json.dumps(json_response, indent=4, sort_keys=True))
    return json_response
