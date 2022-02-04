import json
import time

import requests
from flask import Flask

headersDic = {
    'Authorization': f'Bearer AAAAAAAAAAAAAAAAAAAAAHldUAEAAAAAZWfc8Puhngcu05YyMR9DYRfNcl0%3DmrcqwGb85ve7cgdDevvP6pwetWXPdbTVJvFqYl6kN3VwQhOo0k',
    'User-Agent': 'v2UserLookupPython'
}

app = Flask(__name__)

@app.route('/')
def hello():
    return '<h1>Working!</h1>'

@app.route('/relay/<params>')
def relay(params):
    url = 'https://api.twitter.com/1.1/friends/list.json'
    print(f"{time.ctime()}: params = {params}")

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
        print(f'{time.ctime()}: status code = {response.status_code}')
        if response.status_code != 200:
            raise Exception(response.status_code, response.text)

        print(f"{time.ctime()}: got response.")

        return response.json()
    except Exception as e:
        print(e)
        code, msg = e.args
        return json.dumps({'code': code, 'exception': msg})



# if __name__ == "__main__":
#     s = "[('user_id', '1315544226533236736'), ('count', '200'), ('cursor', '-1')]"
#     relay(s)
