from math import ceil, floor
from pylab import *
import matplotlib.pyplot as plt


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


def main():
    # parse_counts_log()
    # filter_unwanted_users()
    # user_info_parser()
    data_plotter("tweets_count.csv", 2)


if __name__ == '__main__':
    main()
