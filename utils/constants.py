api_key = "HxLdOc22bqKmoo65Gd6zpZFbM"
secret_key = "bN8NQdPvL1lw3aj3FwaxFDKDaLLKxwDx6O8bJ9XzR9itl9RAZN"
bearer_token = "AAAAAAAAAAAAAAAAAAAAAA30TgEAAAAAdtqd1KsuXVp%2F%2BPMWDp0ju%2BiDXxc%3DyBFbdev3Kc505uj7EYSaaUQkH6m4uDcoy1jtjHqHaZLlI34DNP"

positive_hashtags = ["#ally", "#pride", "#lgbtq", "#lgbt", "#socialjustice",
                     "#gay", "#loveislove", "#queer", "#lesbian", "#pridemonth",
                     "#equality", "#h", "#allyship", "#bisexual", "#Transgender",
                     "#LGBTQ", "#PrideMonth", "#PrideMonth2021", "#Pride2021", "#HearQueerYouth",
                     "#pronouns", "#AltogetherDifferent", "#LGBTQIA", "#lgbtq", "#lgbt",
                     "#gay", "#pride", "#loveislove", "#lesbian", "#queer",
                     "#bisexual", "#transgender", "#instagay", "#trans", "#gaypride",
                     "#gayboy", "#lgbtqia", "#pridemonth", "#lgbtpride", "#nonbinary",
                     "#bi", "#dragqueen", "#pansexual", "#gayman", "#genderfluid",
                     "#gaylove", "#asexual", "#lgbtcommunity", "#pansexual", "#bisexual",
                     "#transexual", "#transsexual", "#transman", "#MeQueer"]

negative_hashtags = ["#antigay", "#antilgbt", "#antilgbtq", "#homophobeandproud", "#HomosDNI",
                     "#homophobic", "#SignsYoSonIsGay", "#Gays must die", "#Transphobia", "#TeamHomophobes"]

# double quotation is used to search with full phrase
positive_keywords = ['"Pride month"', '"gay pride"', '"trans pride"']

negative_keywords = ['"homos must die"']

all_tags = []
query_tag_index = 0

tweet_fields = ["id", "text", "author_id", "conversation_id", "created_at", "geo", "public_metrics"]

mongodb_url = "mongodb://localhost:27017/"
mongodb_db_name = "TwitterData"


def construct_query_str(allow_retweet=False, only_en=True):
    global query_tag_index
    global all_tags
    if len(all_tags) == 0:
        all_tags.extend(positive_hashtags)
        all_tags.extend(negative_hashtags)
        all_tags.extend(positive_keywords)
        all_tags.extend(negative_keywords)

    query = ""
    if not allow_retweet:
        query += " -is:retweet"

    if only_en:
        query += " lang:en"

    query = str.strip(query) + " ("
    query_length = len(query)
    _or = ""

    if query_tag_index < len(all_tags):
        for i in range(query_tag_index, len(all_tags)):
            hashtag = all_tags[i]

            if query_length + 4 + len(hashtag) >= 512:
                break

            query += _or + hashtag
            query_tag_index += 1
            query_length += 4 + len(hashtag)
            _or = " OR "

    else:
        raise ValueError("No further tags to search.")

    query += ")"

    return query


def construct_tweet_fields_str():
    tweet_fields_str = ""
    comma = ""
    for field in tweet_fields:
        tweet_fields_str += comma + field
        comma = ","

    return tweet_fields_str
