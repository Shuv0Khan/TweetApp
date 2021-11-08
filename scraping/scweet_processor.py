from Scweet.user import get_user_information, get_users_followers, get_users_following

# const.USERNAME=nowayout1011
# const.PASSWORD=TooMuchWork

def read_users():
    return ['@Shuv0Khan']

def get_user_info():
    env_path = ".env"
    users = read_users()
    # user_info = get_user_information(users, headless=True)
    # user_info_dict = {}
    # for user_id in user_info.keys():
    #     user_info_dict[user_id] = {}
    #     user_info_dict[user_id]['following'] = user_info[user_id][0]
    #     user_info_dict[user_id]['followers'] = user_info[user_id][1]
    #     user_info_dict[user_id]['join_date'] = user_info[user_id][2]
    #     user_info_dict[user_id]['birthday'] = user_info[user_id][3]
    #     user_info_dict[user_id]['location'] = user_info[user_id][4]
    #     user_info_dict[user_id]['website'] = user_info[user_id][5]
    #     user_info_dict[user_id]['desc'] = user_info[user_id][6]

    following = get_users_following(users=users, env=env_path, verbose=0, headless=False, wait=2,  # limit=50,
                                    file_path=None)

    # followers = get_users_followers(users=users, env=env_path, verbose=0, headless=True, wait=2,  # limit=50,
    #                                 file_path=None)
    #
    #
    # print(user_info_dict)
    print(following)
    # print(followers)




if __name__=='__main__':
    get_user_info()
