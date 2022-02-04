import json

import requests
from flask import Flask

headersDic = {
    'Authorization': f'Bearer AAAAAAAAAAAAAAAAAAAAAA30TgEAAAAAdtqd1KsuXVp%2F%2BPMWDp0ju%2BiDXxc%3DyBFbdev3Kc505uj7EYSaaUQkH6m4uDcoy1jtjHqHaZLlI34DNP',
    'User-Agent': 'v2UserLookupPython'
}

app = Flask(__name__)

@app.route('/')
def hello():
    return '<h1>Working!</h1>'

@app.route('/relay/<params>')
def relay(params):
    url = 'https://api.twitter.com/1.1/friends/list.json'
    print(f"params = {params}")

    query_tuple_list = []
    params = params[1:-1]
    for tpl in params.split('),'):
        tpl = tpl.strip()[1:].split(',')
        query_tuple_list.append((tpl[0].strip().replace("'", ""), tpl[1].strip().replace("'", "")))

    print(query_tuple_list)

    try:
        response = requests.get(url,
                                headers=headersDic,
                                params=query_tuple_list)
        print(f'status code = {response.status_code}')
        if response.status_code != 200:
            raise Exception(response.status_code, response.text)

        print("got response.")

        return response.json()
    except Exception as e:
        print(e)
        code, msg = e.args
        return json.dumps({'code': code, 'exception': msg})



# if __name__ == "__main__":
#     s = "[('user_id', '1315544226533236736'), ('count', '200'), ('cursor', '-1')]"
#     relay(s)
