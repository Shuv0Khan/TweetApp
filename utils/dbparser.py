import logging

from core import mongodb_processor, neo4j_processor
from utils import constants


def get_all_tweets():
    mongodb = mongodb_processor.MongoDBProcessor(constants.mongodb_url, constants.mongodb_db_name).my_db
    collections = mongodb.list_collection_names()
    for collection in collections:
        logging.debug("*****************************************************************************")
        logging.debug(f"******** Collection: {collection} *******************************************")
        logging.debug("*****************************************************************************")
        cursor = mongodb[collection].find({}, {"data.text": 1})

        for data_list in cursor:

            for tweet_list in data_list["data"]:
                logging.debug(tweet_list["text"])


def get_all_unique_author_ids():
    mongodb = mongodb_processor.MongoDBProcessor(constants.mongodb_url, constants.mongodb_db_name).my_db
    collections = mongodb.list_collection_names()

    users = {}
    for collection in collections:
        cursor = mongodb[collection].find({}, {"data.author_id": 1})

        for data_list in cursor:
            for tweet_list in data_list["data"]:
                author = tweet_list["author_id"]
                if author not in users.keys():
                    users[author] = 1

    for key in users:
        logging.debug(key)


def get_all_user_metrics():
    mongodb = mongodb_processor.MongoDBProcessor(constants.mongodb_url, constants.mongodb_db_name).my_db
    collection = mongodb.get_collection(name='users_info')
    cursor = mongodb['users_info'].find({}, {
        'data.id': 1, 'data.username': 1, 'location': 1, 'data.public_metrics': 1,
        'protected': 1, 'created_at': 1, 'profile_image_url': 1, 'verified': 1, '_id': 0
    })
    with open("users_metrics.csv", mode="w", encoding="utf8") as out:
        out.write(
            'id, username, location, followers, followings, tweets, listed, protected, created_at, verified, pp_url\n')
        for data_list in cursor:
            for key in data_list:
                for data in data_list[key]:
                    loc = '_NA_'
                    if 'location' in data:
                        loc = data['location']

                    line = str(data['id']) + ', ' + \
                           data['username'] + ', ' + \
                           loc + ', ' + \
                           str(data['public_metrics']['followers_count']) + ', ' + \
                           str(data['public_metrics']['following_count']) + ', ' + \
                           str(data['public_metrics']['tweet_count']) + ', ' + \
                           str(data['public_metrics']['listed_count']) + ', ' + \
                           str(data['protected']) + ', ' + \
                           str(data['created_at']) + ', ' + \
                           str(data['verified']) + ', ' + \
                           str(data['profile_image_url']) + '\n'

                    out.write(line)
                    return


def get_all_users_bio():
    mongodb = mongodb_processor.MongoDBProcessor(constants.mongodb_url, constants.mongodb_db_name).my_db
    collection = mongodb.get_collection(name='users_info')
    cursor = mongodb['users_info'].find({}, {'data.id': 1, 'data.description': 1, 'data.entities': 1, '_id': 0})
    out = open("users_bio.csv", "w")

    for data_list in cursor:
        for key in data_list:
            for data in data_list[key]:
                bio = data['description'].strip()
                bio = bio.replace('\n', ' ')
                out.write(f"{data['id']}\t{bio}\n")

    out.close()


def neo_user_info_transfer():
    userset = {}
    with open("users_with_username.csv", mode="r", encoding="utf8") as unf, open("userset.csv", mode="r",
                                                                                 encoding="utf8") as fset:
        lines = unf.readlines()
        for line in lines:
            parts = line.strip().split(",")
            if len(parts) > 6:
                print(f"problem in line - {line}")
                return
            else:
                userset[parts[0]] = {}
                userset[parts[0]]['take'] = 0
                userset[parts[0]]['username'] = parts[1]
                userset[parts[0]]['followers'] = parts[2]
                userset[parts[0]]['following'] = parts[3]
                userset[parts[0]]['tweets'] = parts[4]

        lines = fset.readlines()
        count = 0
        for line in lines:
            parts = line.strip().split(",")
            if parts[0] in userset:
                count += 1
                userset[parts[0]]['take'] = 1

    print("userset length: ", count)

    with neo4j_processor.Neo4jProcessor() as neo:
        count = 0
        for id in userset.keys():
            if userset[id]['take'] == 1:
                cypher = 'MERGE (a:User {id: "' + id + '"}) ON CREATE SET a.name="' + userset[id][
                    'username'] + '", a.followers=' + userset[id]['followers'] + ', a.following=' + userset[id][
                             'following'] + ', a.tweets=' + userset[id]['tweets']
                # print(f"cypehr = {cypher}")
                # print(f"response = {neo.query(cypher)}")
                neo.query(cypher)
                count += 1
                print(f"--- {count}")


def main():
    logging.basicConfig(filename='DBParser.log',
                        format='%(message)s',
                        level=logging.DEBUG)

    # get_all_unique_author_ids()
    get_all_user_metrics()
    # neo_user_info_transfer()
    # get_all_users_bio()


if __name__ == '__main__':
    main()
