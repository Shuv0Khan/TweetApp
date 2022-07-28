import logging
import traceback

from core import mongodb_processor, neo4j_processor
from utils import constants
from collections import defaultdict


def get_all_tweets():
    """
    Demo of how to connect with mongoDB and iterate through the
    collections.
    """
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


def get_all_unique_tweet_author_ids():
    """
    All the tweets collected are saved in MongoDB with collection names
    corresponding to the day of the Tweet. For example if a Tweet was
    made on 23rd January 2010, the name of the collection with the Tweet
    will be 20100123.

    Each Tweet object, among other metrics, includes the author_id that
    indicates the author of the Tweet. Our primary User set is made from
    collecting all the authors from the collected tweets.
    """

    # TODO: check if the unique user collection working

    mongodb = mongodb_processor.MongoDBProcessor(constants.mongodb_url, constants.mongodb_db_name).my_db
    collections = mongodb.list_collection_names()

    users = defaultdict(int)
    for collection in collections:

        try:
            ''' 
            Tweet Collections are named as dates without separator like - 20220131
            If a Collection name can't be converted to int then that collection
            is used for other purposes like user_info/followers etc.
            '''
            col_to_int = int(collection)
        except Exception as e:
            logging.debug(f"Collection - {collection} skipped")
            continue

        cursor = mongodb[collection].find({}, {"data.author_id": 1})

        for data_list in cursor:
            for tweet_list in data_list["data"]:
                author = tweet_list["author_id"]
                if author not in users:
                    users[author] += 1

    with open("unique_author_ids.csv", mode='w') as fout:
        for key in users:
            fout.write(f'{key}\n')


def get_all_user_metrics():
    """
    Using the Unique Author IDs found from our collected Tweets
    We've queried Tweeter for user information and saved them in
    MongoDB. The collection name is "user_info" and each document
    has an "id" parameter that holds the user id.

    Due to Collection process restarting multiple times there seems
    to be quite a lot of duplicate user information in the DB. Here
    We are collecting relevant user info into one single file for
    further processing.
    """

    # TODO: unique user check. also try to find out why so many duplicates
    # TODO: location newline replace may not be working. check.

    mongodb = mongodb_processor.MongoDBProcessor(constants.mongodb_url, constants.mongodb_db_name).my_db
    collection = mongodb.get_collection(name='users_info')
    cursor = mongodb['users_info'].find({}, {
        'data.id': 1, 'data.username': 1, 'data.location': 1, 'data.public_metrics': 1,
        'data.protected': 1, 'data.created_at': 1, 'data.profile_image_url': 1, 'data.verified': 1, '_id': 0
    })
    with open("users_metrics.csv", mode="w", encoding="utf8") as out:
        out.write(
            'id, username, location, followers, followings, tweets, listed, protected, created_at, verified, pp_url\n')
        for data_list in cursor:
            for key in data_list:
                for data in data_list[key]:
                    '''
                    It is not guaranteed that every user object
                    contains all the parameters. Missing params
                    are indicated by "_NA_"
                    '''

                    user_location = is_protected = created_at = is_verified = pp_url = '_NA_'
                    if 'location' in data:
                        # Since it's a csv file we are replacing all "," and new lines.
                        user_location = data['location'].replace(',', ';')
                        user_location = user_location.replce('\n', ' ')

                    if 'protected' in data:
                        is_protected = str(data['protected'])

                    if 'created_at' in data:
                        created_at = data['created_at']

                    if 'verified' in data:
                        is_verified = str(data['verified'])

                    if 'profile_image_url' in data:
                        pp_url = data['profile_image_url']

                    line = str(data['id']) + ', ' + \
                           data['username'] + ', ' + \
                           user_location + ', ' + \
                           str(data['public_metrics']['followers_count']) + ', ' + \
                           str(data['public_metrics']['following_count']) + ', ' + \
                           str(data['public_metrics']['tweet_count']) + ', ' + \
                           str(data['public_metrics']['listed_count']) + ', ' + \
                           is_protected + ', ' + \
                           created_at + ', ' + \
                           is_verified + ', ' + \
                           pp_url + '\n'

                    out.write(line)


def get_all_users_bio():
    """
    Each User in "user_info" collection has a profile description
    that a Tweeter user can set. These are called user bios.

    Here we are separately collecting all the User Bios. As before
    there are duplicate users in the collection that needs to be
    filtered.
    """

    # TODO: ensure unique users.
    # TODO: check bio new lines. sometimes not working.

    mongodb = mongodb_processor.MongoDBProcessor(constants.mongodb_url, constants.mongodb_db_name).my_db
    collection = mongodb.get_collection(name='users_info')
    cursor = mongodb['users_info'].find({}, {'data.id': 1, 'data.description': 1, 'data.entities': 1, '_id': 0})

    with open("all_user_bios.csv", "w") as out:
        for data_list in cursor:
            for key in data_list:
                for data in data_list[key]:
                    bio = data['description'].strip()
                    bio = bio.replace('\n', ' ')
                    out.write(f"{data['id']}\t{bio}\n")


def neo_user_info_transfer():
    """
    Creating Nodes in neo4j DB for users selected
    for Follower/Following collection. This is done
    to build the network in parallel to Follower/Following
    collection.

    'userset.csv' file is checked for duplicate users
    separately before starting this operation.
    """

    userset = {}
    with open("users_with_username.csv", mode="r", encoding="utf8") as uu_fin, open(
            "userset.csv", mode="r", encoding="utf8") as uset_fin:

        # Reading all users and their metrics
        for line in uu_fin:
            parts = line.strip().split(",")
            if len(parts) > 6:
                # There was some formatting issues in the file.
                # Those were fixed manually one-by-one, stopping
                # here each time one was found.
                print(f"problem in line - {line}")
                return
            else:
                userset[parts[0]] = {}
                userset[parts[0]]['take'] = 0
                userset[parts[0]]['username'] = parts[1]
                userset[parts[0]]['followers'] = parts[2]
                userset[parts[0]]['following'] = parts[3]
                userset[parts[0]]['tweets'] = parts[4]

        # Cross-checking with selected user set and marking the users.
        count = 0
        for line in uset_fin:
            parts = line.strip().split(",")
            if parts[0] in userset:
                count += 1
                userset[parts[0]]['take'] = 1

    print("userset length: ", count)

    with neo4j_processor.Neo4jProcessor() as neo:
        count = 0
        for id in userset.keys():
            if userset[id]['take'] == 1:
                # Using MERGE-CREATE to ensure that never two users with same id is created.

                cypher = 'MERGE (a:User {id: "' + id + '"}) ON CREATE SET a.name="' + userset[id][
                    'username'] + '", a.followers=' + userset[id]['followers'] + ', a.following=' + userset[id][
                             'following'] + ', a.tweets=' + userset[id]['tweets']
                # print(f"cypehr = {cypher}")
                # print(f"response = {neo.query(cypher)}")
                neo.query(cypher)
                count += 1
                print(f"--- {count}")


def neo_following_edges_cmd_gen():
    users_created = defaultdict(int)
    users_metrics = {}
    with open('temp_users_in_neo.csv', mode='r') as fin:
        for line in fin:
            users_created[line.strip()] = 1

    with open('users_metrics.csv', mode='r', encoding='utf8') as fin:
        for line in fin:
            parts = line.strip().split(',')
            if line.startswith('id'):
                continue

            try:
                if parts[0] not in users_metrics:
                    users_metrics[parts[0]] = {
                        'id': parts[0],
                        'name': parts[1],
                        'followers': int(parts[3]),
                        'following': int(parts[4]),
                        'tweets': int(parts[5])
                    }
            except Exception as e:
                print(line)

    mongodb = mongodb_processor.MongoDBProcessor(constants.mongodb_url, constants.mongodb_db_name).my_db
    collection = mongodb.get_collection(name='followings')
    cursor = mongodb['followings'].find({})

    total = 0
    limit = 1000
    for doc in cursor:
        logging.debug('\n************************************')
        logging.debug(f'\tfollowings for user : {doc["user_id"]}')
        logging.debug('************************************\n')

        total += 1

        if doc['user_id'] not in users_created:
            # create user

            if doc['user_id'] in users_metrics:
                user = users_metrics[doc['user_id']]
            else:
                user = {
                    'name': doc['user_id'],
                    'id': doc['user_id'],
                    'followers': -1,
                    'following': len(doc['followings']),
                    'tweets': -1,
                    'invalid_name': 1
                }

            cypher = 'CREATE   (:User {id: "' + str(user["id"]) + \
                     '", name: "' + user["name"].strip() + \
                     '", followers: ' + str(user["followers"]) + \
                     ', following: ' + str(user["following"]) + \
                     ', tweets: ' + str(user["tweets"]) + \
                     '})'

            users_created[doc['user_id']] += 1

            logging.debug('\n' + cypher + '\n\n')

        for following in doc['followings']:

            total += 1

            if following['id_str'] not in users_created:
                # create following user
                cypher = 'CREATE (:User {id: "' + following["id_str"] + \
                         '", name: "' + following["name"] + \
                         '", followers: ' + str(following["followers_count"]) + \
                         ', following: ' + str(following["friends_count"]) + \
                         ', tweets: -1' + \
                         '})'

                users_created[following['id']] += 1

                logging.debug(cypher + '\n')

            # create directed edge from user->following
            cypher = 'CREATE (:User {id: "' + doc["user_id"] + '"})-[:Following]->(:User {id: "' + following["id_str"] + '"})'
            logging.debug(cypher + '\n')

        if total > limit:
            break

def get_all_tweets_and_metrics():
    mongodb = mongodb_processor.MongoDBProcessor(constants.mongodb_url, constants.mongodb_db_name).my_db
    collections = mongodb.list_collection_names()
    collections.sort()
    with open('../data/all_tweets.tsv', mode='w', encoding='utf8') as tout, open(
            '../data/all_tweet_metrics.csv', mode='w', encoding='utf8') as tmout:
        tmout.write(f'id,author_id,retweets,replies,likes,quotes,created_at\n')
        for collection in collections:
            try:
                int(collection)
            except Exception:
                continue

            print("*****************************************************************************")
            print(f"******** Collection: {collection} *******************************************")
            print("*****************************************************************************")
            # cursor = mongodb[collection].find({}, {"data.text": 1})
            cursor = mongodb[collection].find({}, {'data': 1})

            for data_list in cursor:

                for tweet_list in data_list["data"]:
                    # print(tweet_list["text"])
                    tweet = tweet_list['text'].strip()
                    tweet = tweet.replace('\t', ' ')
                    tweet = tweet.replace('\n', ' ')
                    tweet = tweet.replace('\r', ' ')
                    tout.write(f'"{tweet_list["id"]}"\t{tweet}\t\n')
                    tmout.write(f'"{tweet_list["id"]}",{tweet_list["author_id"]},{tweet_list["public_metrics"]["retweet_count"]},{tweet_list["public_metrics"]["reply_count"]},{tweet_list["public_metrics"]["like_count"]},{tweet_list["public_metrics"]["quote_count"]},"{tweet_list["created_at"]}"\n')


def main():
    logging.basicConfig(filename='following_cypher.cql',
                        format='%(message)s',
                        level=logging.DEBUG)

    # get_all_unique_tweet_author_ids()
    # get_all_user_metrics()
    # neo_user_info_transfer()
    # get_all_users_bio()
    # neo_following_edges_cmd_gen()
    get_all_tweets_and_metrics()


if __name__ == '__main__':
    main()
