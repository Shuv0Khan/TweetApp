






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








def main():
    parse_counts_log()



if __name__ == '__main__':
    main()