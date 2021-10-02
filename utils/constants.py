from datetime import date, timedelta
from searchtweets import convert_utc_time

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

expansions = ["author_id", "entities.mentions.username", "geo.place_id", "in_reply_to_user_id",
              "referenced_tweets.id", "referenced_tweets.id.author_id"]
tweet_fields = ["id", "text", "author_id", "conversation_id", "created_at", "geo", "in_reply_to_user_id",
                "public_metrics", "entities", "possibly_sensitive", "referenced_tweets"]
user_fields = ["created_at", "id", "location", "name", "pinned_tweet_id", "protected", "url", "username", "verified"]
place_fields = ["id", "full_name", "country", "geo", "place_type"]
query_length = 512
max_results = 500

mongodb_url = "mongodb://localhost:27017/"
mongodb_db_name = "TwitterDataFullArchive"


def construct_query_str(allow_retweet=False, only_en=True):
    global query_length
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
    q_len = len(query)
    _or = ""

    if query_tag_index < len(all_tags):
        for i in range(query_tag_index, len(all_tags)):
            hashtag = all_tags[i]

            if q_len + 4 + len(hashtag) >= query_length:
                break

            query += _or + hashtag
            query_tag_index += 1
            q_len += 4 + len(hashtag)
            _or = " OR "

    else:
        raise ValueError("No further tags to search.")

    query += ")"

    return query


def construct_csv_str(items=[]):
    csv_str = ""
    comma = ""
    for item in items:
        csv_str += comma + item
        comma = ","

    return csv_str


def construct_expansion_str():
    return construct_csv_str(expansions)


def construct_tweet_fields_str():
    return construct_csv_str(tweet_fields)


def construct_user_fields_str():
    return construct_csv_str(user_fields)


def construct_place_fields_str():
    return construct_csv_str(place_fields)


def reset_query_index():
    global query_tag_index
    query_tag_index = 0


def construct_query_param(start_time=date.fromisoformat("2008-01-01"), inc=timedelta(days=1)):
    end_time = start_time + inc

    query_params = {
        "query": construct_query_str(),
        "expansions": construct_expansion_str(),
        "tweet.fields": construct_tweet_fields_str(),
        "user.fields": construct_user_fields_str(),
        "place.fields": construct_place_fields_str(),
        "max_results": max_results,
        "start_time": convert_utc_time(start_time.isoformat()),
        "end_time": convert_utc_time(end_time.isoformat())
    }
    return query_params
