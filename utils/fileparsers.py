import traceback
from collections import defaultdict

import requests
import tldextract
from pylab import *


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
    with open("users_bio.csv", mode="r", encoding="utf8") as fin, open(
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


def main():
    # parse_counts_log()
    # filter_unwanted_users()
    # user_info_parser()
    # user_set_generation()
    user_bio_parser()
    # users_bio_url_resolver()
    # users_bio_url_domains()


if __name__ == '__main__':
    main()
