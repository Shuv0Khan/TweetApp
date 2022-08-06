import traceback
from collections import defaultdict

import pandas as pd
import requests
import tldextract
from consolemenu import SelectionMenu
from nltk.tokenize import TweetTokenizer
from pylab import *
from tqdm import tqdm
import constants
import os


# pd.options.plotting.backend = "plotly"


def parse_counts_log():
    file = open("../counts.log", "r")
    lines = file.readlines()

    day = ""
    count = ""
    daily_dict = {}
    monthly_dict = {}
    yearly_dict = {}
    for line in lines:
        if "start_time" in line:
            day = line.split(":")[1].split("T")[0].strip()[1:]
            month = day.split("-")
            year = month[0]
            month = f"{year}-{month[1]}"

            if day not in daily_dict:
                daily_dict[day] = 0
            if month not in monthly_dict:
                monthly_dict[month] = 0
            if year not in yearly_dict:
                yearly_dict[year] = 0

        elif "ending" in line:
            count = line.split("at")[1].strip().split()[0]
            month = day.split("-")
            year = month[0]
            month = f"{year}-{month[1]}"

            daily_dict[day] = daily_dict[day] + int(count)
            monthly_dict[month] = monthly_dict[month] + int(count)
            yearly_dict[year] = yearly_dict[year] + int(count)

    # for key in daily_dict.keys():
    #     print(f"{key},{daily_dict[key]}")
    #
    # for key in monthly_dict.keys():
    #     print(f"{key},{monthly_dict[key]}")
    #

    for key in yearly_dict.keys():
        print(f"{key},{yearly_dict[key]}")


def filter_unwanted_users():
    with open("C:\\Users\\shuvo\\OneDrive\\Studies\\MSc\\GA\\Works\\users.csv", mode="r", encoding="utf8") as f1, open(
            "C:\\Users\\shuvo\\OneDrive\\Studies\\MSc\\GA\\Works\\Unique Users.log", mode="r",
            encoding="utf8") as f2, open("uusers.csv", "w") as oup:
        lines = f2.readlines()
        users = {}
        for line in lines:
            users[line.strip()] = 1

        lines = f1.readlines()
        i = 0
        for line in lines:
            parts = line.strip().split(",")
            if parts[0] in users:
                oup.write(f"{line.strip()}\n")
                i += 1

        print(f"users = {i}")


def user_info_parser():
    with open("uusers.csv", mode="r", encoding="utf8") as inp, open(
            "followers.csv", mode="w", encoding="utf8") as out_followers, open(
        "following.csv", mode="w", encoding="utf8") as out_following, open(
        "tweets_count.csv", mode="w", encoding="utf8") as out_tweets:

        lines = inp.readlines()
        counts = []
        maxim = -1
        i = 1
        for line in lines:
            if "id" in line:
                continue
            try:
                parts = line.strip().split(",")

                out_followers.write(f"{i},{parts[0]},{int(parts[1])}\n")
                out_following.write(f"{i},{parts[0]},{int(parts[2])}\n")
                out_tweets.write(f"{i},{parts[0]},{int(parts[3])}\n")

                i += 1

                # counts.append(followers)
                # if followers > maxim:
                #     maxim = followers

            except Exception as e:
                print(line)
                raise e

        # print(f"max = {maxim}")
        # distribution = [0] * (int(maxim / 10000) + 1)
        # print(f"total data points {len(distribution)}")

        # for c in counts:
        #     index = int(c / 10000)
        #     distribution[index] += 1

        # for i in range(len(distribution)):
        #     out_followers.write(f"{i + 1},{distribution[i]}\n")


def user_set_generation():
    with open("followers.csv", mode="r", encoding="utf8") as in_followers, open(
            "following.csv", mode="r", encoding="utf8") as in_following, open("userset2.csv", mode="w") as out_f:
        lines = in_followers.readlines()
        set_b = {}
        for line in lines:
            parts = line.strip().split(",")
            followers = int(parts[2])

            # if followers <= 1500:
            set_b[parts[1]] = {}
            set_b[parts[1]]['take'] = 0
            set_b[parts[1]]['followers'] = followers

        lines = in_following.readlines()
        for line in lines:
            parts = line.strip().split(",")
            followings = int(parts[2])

            if parts[1] in set_b and followings <= 400:
                set_b[parts[1]]['take'] = 1
                set_b[parts[1]]['followings'] = followings

        total_users = 0
        for key in set_b.keys():
            if set_b[key]['take'] == 1:
                out_f.write(f"{key},{set_b[key]['followers']},{set_b[key]['followings']}\n")
                total_users += 1

        print(f"total users = {total_users}")


def user_bio_parser():
    with open("all_user_bios.csv", mode="r", encoding="utf8") as fin, open(
            "words_with_emoji.txt", mode="w", encoding="utf8") as fout1, open(
        "hashtags.txt", mode="w", encoding="utf8") as fout2, open(
        "urls.txt", mode="w", encoding="utf8") as fout3:
        users_wout_bio = 0
        session = requests.Session()
        for line in fin:
            try:
                parts = line.split("\t")
                if len(parts) == 1:
                    users_wout_bio += 1
                    continue

                line = parts[1].strip()

                # Remove non-english characters
                # source - https://www.geeksforgeeks.org/python-remove-non-english-characters-strings-from-list/
                # line = re.sub("[^\u0000-\u05C0\u2100-\u214F]+", " ", line)

                # Remove links
                match = re.search(r'https?:\S+', line)
                while match is not None:
                    url = line[match.start():match.end()]
                    fout3.write(f'{url}\n')
                    line = f'{line[0:match.start()]} {line[match.end():]}'
                    match = re.search(r'https?:\S+', line)

                line = re.sub(r'https?:\S+', ' ', line)

                # Lowercase all words
                line = line.lower()

                fout1.write(f'{parts[0]}\t')

                words = line.split()
                for word in words:
                    if word.startswith("@"):
                        continue
                    elif word.startswith("#"):
                        fout2.write(f'{word} ')
                    else:
                        fout1.write(f'{word} ')

                fout1.write("\n")
                fout2.write("\n")

            except Exception:
                traceback.print_exc()
                print(line)
                exit(1)

        print(f'users without bio: {users_wout_bio}')


def unique_user_bio_selection():
    users = {}
    skipped_count = 0
    with open("words_with_emoji.txt", mode='r', encoding='utf8') as fin:
        for line in fin:
            parts = line.strip().split('\t')
            if len(parts) == 1:
                continue

            if (parts[0] not in users) or (users[parts[0]] != parts[1]):
                users[parts[0]] = parts[1]
            else:
                skipped_count += 1

    with open("words_with_emoji.txt", mode='w', encoding='utf8') as fout:
        for key in users:
            fout.write(f'{key}\t{users[key]}\n')

    print(f"Total unique bios: {len(users)}")
    print(f"Skipped: {skipped_count}")


def users_bio_url_resolver():
    with open("urls.txt", mode="r", encoding="utf8") as fin, open(
            "resolved_urls.txt", mode="a", encoding="utf8") as fout:
        urls = fin.readlines()
        session = requests.Session()
        current_index = 45068
        end_index = len(urls)
        while current_index < end_index:
            url = urls[current_index]
            current_index += 1
            resolved_url = unshorten_url(url.strip())
            fout.write(f'{resolved_url}\n')


def unshorten_url(url='') -> str:
    if len(url) == 0:
        return url
    i = -1
    while re.match(r'[A-Z|a-z|\d|/]', url[i]) is None:
        i -= 1
    i += 1

    if i == 0:
        cleaned_url = url
    else:
        cleaned_url = url[:i]

    resolved_url = ''
    try:
        resp = requests.get(cleaned_url, timeout=3, verify=False, stream=True)
        resolved_url = resp.url
    except Exception as e:
        traceback.print_exc()
        print(f'{url}\t{cleaned_url}')
        exp = str(e.args)
        match = exp.find('host=')
        if match > 0:
            resolved_url = exp[match:]
            resolved_url = resolved_url.split(",")[0]
            resolved_url = resolved_url[6:-1]

    return f'{url}\t{cleaned_url}\t{resolved_url}'


def users_bio_url_domains():
    domain_count = defaultdict(int)
    with open("resolved_urls.txt", mode="r", encoding="utf8") as fin:
        for line in fin:
            parts = line.strip().split("\t")
            if len(parts) == 3:
                url = parts[2]
                if not url.startswith("http"):
                    url = f'https://{url}'
                url = tldextract.extract(url)
                if url.domain is None:
                    print(line)
                domain_count[url.domain] += 1
    sorted_dom_count = dict(sorted(domain_count.items(), key=lambda t: t[1], reverse=True))

    with open("url_counts.csv", mode="w", encoding="utf8") as fout:
        for k, v in sorted_dom_count.items():
            fout.write(f'{k},{v}\n')


def users_bio_url_domains_visualization():
    df = pd.read_csv('url_counts_from_bios.csv', header=None)
    df.columns = ['domains', 'counts']
    print(df)
    print(df.info())
    print(df[df['domains'].isna()])
    df['domains'].fillna("notavailable", inplace=True)
    total = df["counts"].sum()
    df['percentage'] = (df['counts'] * 100.0) / total
    print(df.head(10))
    df[:20].plot.bar(x='domains', y='counts')
    plt.show()
    df[:20].plot.hist(y='counts')
    plt.show()


def users_bio_url_domains_classification():
    kaggle_df = pd.read_csv('kaggle_url_classification.csv')

    # There are some NaN rows in the data
    kaggle_df.dropna(inplace=True)
    kaggle_df.drop_duplicates(inplace=True)
    kaggle_df.columns = ['sl', 'urls', 'category']

    # List of categories per domain for multiple categorizations
    domain_cate = dict()

    for i in kaggle_df.index:
        try:
            url = tldextract.extract(kaggle_df['urls'][i])
            if url.domain is None:
                print(kaggle_df['urls'][i])

            if url.domain not in domain_cate:
                domain_cate[url.domain] = set()

            domain_cate[url.domain].add(kaggle_df['category'][i])

        except Exception:
            traceback.print_exc()
            print(kaggle_df.loc[i])

    twitter_df = pd.read_csv('url_counts_from_bios.csv', header=None)
    twitter_df.columns = ['domains', 'counts']
    twitter_df['category'] = ''

    for i in twitter_df.index:
        if twitter_df['domains'][i] not in domain_cate:
            cate = None
        else:
            cate = domain_cate[twitter_df['domains'][i]]

        if cate is None:
            cate = set('NaN')

        twitter_df['category'].loc[i] = ','.join(cate)

    twitter_df.to_csv('url_kaggle_classes.csv')


def replace_char(org, replace, in_file):
    with open(in_file, mode='r', encoding='utf8') as fin, open('temp.dat', mode='w', encoding='utf8') as fout:
        for line in tqdm(fin):
            line = line.replace(org, replace)
            fout.write(line)

    os.rename(in_file, f'{in_file}_{time.time()}')
    os.replace('temp.dat', in_file)


def tweet_parser():
    tokenizer = TweetTokenizer(preserve_case=False, strip_handles=True, reduce_len=True)

    # hashtags = defaultdict(int)
    # for tag in constants.positive_hashtags:
    #     hashtags[str.lower(tag)] = 1
    #
    # for tag in constants.negative_hashtags:
    #     hashtags[str.lower(tag)] = 1
    #

    with open("../data/all_tweets.tsv", mode="r", encoding="utf8") as fin, open(
            "../data/all_tweets_words_with_emoji.tsv", mode="w", encoding="utf8") as t_out, open(
        "../data/all_tweets_hashtags.txt", mode="w", encoding="utf8") as h_out, open(
        "../data/all_tweets_urls.txt", mode="w", encoding="utf8") as url_out:
        empty_tweets = 0
        for line in tqdm(fin):
            try:
                parts = line.strip().split("\t")
                if len(parts) == 1:
                    empty_tweets += 1
                    continue

                line = parts[1].strip()

                # Remove non-english characters
                # source - https://www.geeksforgeeks.org/python-remove-non-english-characters-strings-from-list/
                # line = re.sub("[^\u0000-\u05C0\u2100-\u214F]+", " ", line)

                # Find and write links
                match = re.search(r'https?:\S+', line)
                while match is not None:
                    url = line[match.start():match.end()]
                    url_out.write(f'{url}\n')
                    line = f'{line[0:match.start()]} {line[match.end():]}'
                    match = re.search(r'https?:\S+', line)

                # Remove Links
                line = re.sub(r'https?:\S+', ' ', line)

                # Write the tweet id
                t_out.write(f'{parts[0]}\t')

                # Tokenize with - lowercase, stripping handles and reducing len to 3 for repeated letters
                words = tokenizer.tokenize(line)

                hashtag_found = False
                for word in words:
                    if word.startswith("#"):
                        # Keeping hashtags as words by removing '#'
                        hashtag_found = True
                        h_out.write(f'{word} ')

                        # Remove hashtags that were used to retrieve the tweets
                        # if word in hashtags:
                        #     continue

                        word = word.replace('#', '')

                    t_out.write(f'{word} ')

                t_out.write("\n")

                if hashtag_found:
                    h_out.write("\n")

            except Exception:
                traceback.print_exc()
                print(line)
                exit(1)

        print(f'empty tweets: {empty_tweets}')


def tweet_distilbert_validation():
    tweets = dict()
    with open('../data/tweet_words_with_emoji.tsv', mode='r', encoding='utf8') as fin:
        for line in fin:
            parts = line.strip().split('\t')
            if parts[0] not in tweets:
                tweets[parts[0]] = dict([('count', 1), ('text', parts[1].strip())])
            else:
                tweets[parts[0]]['count'] += 1

    retweet_count = 0
    for [key, val] in tweets.items():
        if val['count'] > 1:
            retweet_count += val['count'] - 1

    print(f'duplicate analysis = {retweet_count}')

    with open('../data/tweet_distilbert.csv', mode='r', encoding='utf8') as fin:
        for line in fin:
            parts = line.strip().split(',')
            if parts[0] in tweets:
                tweets[parts[0]]['count'] -= 1

    with open('../data/tweet_words_with_emoji_remaining.tsv', mode='w', encoding='utf8') as fout:
        for [key, val] in tweets.items():
            if val['count'] > 0:
                for i in range(val['count']):
                    fout.write(f'{key}\t{val["text"]}\n')


def tweets_fix_tsv_issues():
    with open('../data/all_tweets.tsv', mode='r', encoding='utf8') as fin:
        lines = fin.readlines()
        issue_count = 0

        for i in range(len(lines)):
            parts = lines[i].strip().split('\t')
            if len(parts) != 2:
                issue_count += 1

        print(f'Total Issues found: {issue_count}')


def tweets_during_peaks():
    peak_months = defaultdict()
    with open('../data/all_tweets_detected_peaks.csv', mode='r', encoding='utf8') as fin:
        for line in fin:
            parts = line.strip().split(',')
            peak_months[parts[1]] = []

    pbar = tqdm(total=8581628)
    with open('../data/all_tweet_metrics.csv', mode='r', encoding='utf8') as fin:
        count = 0
        index = 0
        for line in fin:
            parts = line.strip().split(',')
            date_str = parts[-1]
            year_month = date_str[1:8]

            if year_month in peak_months:
                peak_months[year_month].append(index)
                count += 1

            index += 1
            pbar.update(index)

    print(f'\nTotal Tweets selected: {count}')

    with open('../data/all_tweets.tsv', mode='r', encoding='utf8') as fin, open(
            '../data/all_tweets_during_peak.tsv', mode='w', encoding='utf8') as fout:
        lines = fin.readlines()
        pbar = tqdm(total=count)
        count = 0

        for d_key, d_val in peak_months.items():
            for i in d_val:
                try:
                    parts = lines[i].strip().split('\t')
                    fout.write(f'{parts[0]}\t{d_key}\t{parts[1]}\n')

                    count += 1
                    pbar.update(count)
                except Exception as e:
                    # traceback.print_stack(e)
                    print(f'For line - {i}: {lines[i]}')
                    exit(i)


def user_activity():
    user_date = defaultdict(dict)
    with open('../data/users_bio_distilbert.csv', mode='r', encoding='ISO-8859-1') as fin:
        for line in fin:
            if line.startswith('id'):
                continue

            parts = line.strip().split(',')

            user_date[parts[0]] = dict(
                positive=(parts[-1] in ['joy', 'love', 'surprise'])
            )

    positive = 0
    negative = 0
    for [k, v] in user_date.items():
        if v['positive']:
            positive += 1
        else:
            negative += 1

    with open('../data/all_users_all_metrics.csv', mode='r', encoding='utf8') as fin:
        for line in fin:
            if line.startswith('id'):
                continue

            parts = line.strip().split(',')
            if parts[0] in user_date:
                try:
                    if parts[0] in user_date:
                        creation_date = datetime.datetime.strptime(parts[-3].strip().split('.')[0],
                                                                   '%Y-%m-%dT%H:%M:%S').date()
                        today = datetime.datetime.strptime('2020-12-31', '%Y-%m-%d').date()
                        user_date[parts[0]]['days'] = (today - creation_date).days
                        user_date[parts[0]]['tweet_count'] = parts[5].strip()
                except Exception:
                    print(line)
                    exit()

    with open('../data/all_unique_users_active_days.csv', mode='w', encoding='utf8') as fout:
        fout.write('id,is_positive,active_days,tweet_count\n')
        line_no = 1
        for [k, v] in user_date.items():
            line_no += 1
            try:
                fout.write(f'{k},{1 if v["positive"] else 0},{v["days"]},{v["tweet_count"]}\n')
            except Exception:
                print(f'{k}   {str(v)}   {line_no}')


def menu():
    options = [
        'Parse Counts Log',
        'Filter Unwanted Users',
        'User Info Parser',
        'User Set Generation',
        'User Bio Parser',
        'Unique User Bio Selection',
        'Users Bio URL Resolver',
        'Users Bio URL Domains',
        'Users Bio URL Domains Classification',
        'Replace Char',
        'Tweet Parser',
        'Tweet DistilBert Validation',
        'User Activity',
        'Tweets During Peaks Parsing',
        'Tweets Fix TSV Issues',
    ]
    m = SelectionMenu(options, "File Parsing Options")
    m.show()
    selection = m.selected_option

    if selection == 0:
        parse_counts_log()
    elif selection == 1:
        filter_unwanted_users()
    elif selection == 2:
        user_info_parser()
    elif selection == 3:
        user_set_generation()
    elif selection == 4:
        user_bio_parser()
    elif selection == 5:
        unique_user_bio_selection()
    elif selection == 6:
        users_bio_url_resolver()
    elif selection == 7:
        users_bio_url_domains()
    elif selection == 8:
        users_bio_url_domains_classification()
    elif selection == 9:
        replace_char(' ’ ', '’', '../data/all_tweets_during_peak_words_with_emoji.tsv')
    elif selection == 10:
        tweet_parser()
    elif selection == 11:
        tweet_distilbert_validation()
    elif selection == 12:
        user_activity()
    elif selection == 13:
        tweets_during_peaks()
    elif selection == 14:
        tweets_fix_tsv_issues()
    else:
        exit(selection)


if __name__ == '__main__':
    print('starting....')
    menu()
    print('done')
