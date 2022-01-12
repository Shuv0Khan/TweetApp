import traceback
from math import ceil, floor

import nltk
from pylab import *
import matplotlib.pyplot as plt
import re
from nltk import word_tokenize
import wordcloud
from wordcloud import WordCloud
from collections import defaultdict

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


def data_plotter(filepath, column):
    with open(filepath, "r") as inp:
        lines = inp.readlines()
        d_points = []

        for line in lines:
            parts = line.strip().split(",")
            d_points.append(int(parts[column]))

        d_points.sort()
        print(f"Max = {d_points[len(d_points) - 1]}")
        # x_points = [i for i in range(1, len(d_points) + 1)]
        # plt.plot(x_points, d_points)
        # plt.show()
        # return

        y_points = [i for i in d_points if 10000 > i > 0]
        y_points.sort()
        # x_points = [i for i in range(1, len(y_points) + 1)]
        # plt.plot(x_points, y_points)
        # plt.show()
        # return

        # min-max normalization
        # y_min = y_points[0]
        # y_max = y_points[-1]
        # for i in range(len(y_points)):
        #     y_points[i] = ((y_points[i] - y_min) / (y_max - y_min))

        # categorize per 100 followers into one group
        group_lim = 100
        total_users = len(y_points)
        print(f"total users = {total_users}")
        cate_y_points = []
        count = 0
        for p in y_points:
            if p < group_lim:
                count += 1
            else:
                # normalizing to percentage of users
                cate_y_points.append(count / total_users * 100.0)
                # count = 0 # commented as I'm doing cumulative sum.
                group_lim += 100

        # column sum for cdf
        # sum = 0
        # y_cumsum = []
        # for p in y_points:
        #     sum += p
        #     y_cumsum.append(sum)

        x_points = [i for i in range(1, len(cate_y_points) + 1)]

        plt.plot(x_points, cate_y_points)
        # plt.plot(x_points, y_cumsum, "r--")
        plt.show()

        count = 100
        for i in cate_y_points:
            print(f"<{count}, {i}%")
            count += 100

def user_set_generation():
    with open("followers.csv", mode="r", encoding="utf8") as in_followers, open(
            "following.csv", mode="r", encoding="utf8") as in_following, open("userset.csv", mode="w") as out_f:
        lines = in_followers.readlines()
        set_b = {}
        for line in lines:
            parts = line.strip().split(",")
            followers = int(parts[2])

            if followers <= 1500:
                set_b[parts[1]] = {}
                set_b[parts[1]]['take'] = 0
                set_b[parts[1]]['followers'] = followers

        lines = in_following.readlines()
        for line in lines:
            parts = line.strip().split(",")
            followings = int(parts[2])

            if parts[1] in set_b and followings <= 1500:
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
            "words.txt", mode="w", encoding="utf8") as fout1, open(
            "hashtags.txt", mode="w", encoding="utf8") as fout2:
        users_wout_bio = 0
        for line in fin:
            try:
                parts = line.split("\t")
                if len(parts) == 1:
                    users_wout_bio += 1
                    continue

                line = parts[1]

                # Remove links
                line = re.sub(r'https?:\S+', ' ', line)

                # Remove non-english characters
                # source - https://www.geeksforgeeks.org/python-remove-non-english-characters-strings-from-list/
                line = re.sub("[^\u0000-\u05C0\u2100-\u214F]+", " ", line)

                # Lowercase all words
                line = line.lower()

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

def users_bio_word_cloud():
    with open("words.txt", mode="r", encoding="utf8") as fwords, open(
            "hashtags.txt", mode="r", encoding="utf8") as fhashs:
        rawtxt = fwords.readlines()
        rawhashs = fhashs.readlines()

        # Tokenize, lemmatize and count frequencies
        lemmatizer = nltk.WordNetLemmatizer()
        stop_words = set(nltk.corpus.stopwords.words('english'))
        word_freq = defaultdict(int)
        lword_freq = defaultdict(int)

        for line in rawtxt:
            line = line.strip()

            if len(line) == 0:
                continue

            # Tokenize lines
            words = [w for w in word_tokenize(line) if w.isalpha() and w not in stop_words]

            # Remove numbers and Lemmatize the words to roots
            lemmatized_words = [lemmatizer.lemmatize(w) for w in words]

            for w in words:
                word_freq[w] += 1

            for lw in lemmatized_words:
                lword_freq[lw] += 1

        hash_freq = defaultdict(int)

        for line in rawhashs:
            line = line.strip()

            if len(line) == 0:
                continue

            # Tokenize lines
            words = [w for w in word_tokenize(line) if w.isalpha()]

            for w in words:
                hash_freq[f'#{w}'] += 1


        wordcloud = WordCloud(max_font_size=50, max_words=100, background_color="white").generate_from_frequencies(word_freq)
        plt.figure()
        plt.imshow(wordcloud, interpolation="bilinear")
        plt.axis("off")

        lwordcloud = WordCloud(max_font_size=50, max_words=100, background_color="white").generate_from_frequencies(lword_freq)
        plt.figure()
        plt.imshow(lwordcloud, interpolation="bilinear")
        plt.axis("off")

        hwordcloud = WordCloud(max_font_size=50, max_words=100, background_color="white").generate_from_frequencies(hash_freq)
        plt.figure()
        plt.imshow(hwordcloud, interpolation="bilinear")
        plt.axis("off")

        plt.show()



def main():
    # parse_counts_log()
    # filter_unwanted_users()
    # user_info_parser()
    # data_plotter("following.csv", 2)
    # user_set_generation()
    # user_bio_parser()
    users_bio_word_cloud()


if __name__ == '__main__':
    main()
