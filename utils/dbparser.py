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


def get_all_user_follower_following_counts():
    mongodb = mongodb_processor.MongoDBProcessor(constants.mongodb_url, constants.mongodb_db_name).my_db
    collection = mongodb.get_collection(name='users_info')
    cursor = mongodb['users_info'].find({}, {'data.id': 1, 'data.username': 1, 'data.public_metrics': 1, '_id': 0})
    out_wname = open("users_with_name.csv", "w")
    out_wt = open("users.csv", "w")

    for data_list in cursor:
        for key in data_list:
            for data in data_list[key]:
                out_wname.write(
                    f"{data['id']},{data['username']},{data['public_metrics']['followers_count']},{data['public_metrics']['following_count']},{data['public_metrics']['tweet_count']},{data['public_metrics']['listed_count']}\n")
                out_wt.write(
                    f"{data['id']},{data['public_metrics']['followers_count']},{data['public_metrics']['following_count']},{data['public_metrics']['tweet_count']}\n")

    out_wname.close()
    out_wt.close()

def neo_user_info_transfer():
    userset={}
    with open("users_with_username.csv", mode="r", encoding="utf8") as unf, open("userset.csv", mode="r", encoding="utf8") as fset:
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
                cypher = 'MERGE (a:User {id: "'+id+'"}) ON CREATE SET a.name="'+userset[id]['username']+'", a.followers='+userset[id]['followers']+', a.following='+userset[id]['following']+', a.tweets='+userset[id]['tweets']
                # print(f"cypehr = {cypher}")
                # print(f"response = {neo.query(cypher)}")
                neo.query(cypher)
                count+=1
                print(f"--- {count}")


def main():
    logging.basicConfig(filename='DBParser.log',
                        format='%(message)s',
                        level=logging.DEBUG)

    # get_all_unique_author_ids()
    # get_all_user_follower_following_counts()
    neo_user_info_transfer()


if __name__ == '__main__':
    main()
