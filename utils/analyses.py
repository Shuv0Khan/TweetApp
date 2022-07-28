import traceback
from collections import defaultdict

import matplotlib.pyplot as plt
import nltk
import pandas as pd
# pd.options.plotting.backend = "plotly"
# pd.options.display.max_colwidth=160
from flair.data import Sentence
from flair.models import TextClassifier
from nltk import word_tokenize
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from transformers import pipeline, AutoTokenizer
from wordcloud import WordCloud


def data_plotter(filepath, column):
    with open(filepath, "r") as inp:
        lines = inp.readlines()
        d_points = []

        for line in lines:
            parts = line.strip().split(",")
            d_points.append(int(parts[column]))

        d_points.sort()
        print(f"Max = {d_points[len(d_points) - 1]}")
        y_points = d_points
        # x_points = [i for i in range(1, len(d_points) + 1)]
        # plt.plot(x_points, d_points)
        # plt.show()
        # return

        # y_points = [i for i in d_points if 10000 > i > 0]
        # y_points.sort()
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
        group_lim = inc = 10000
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
                group_lim += inc

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

        count = inc
        for i in cate_y_points:
            print(f"<{count}, {i}%")
            count += inc


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

        wordcloud = WordCloud(max_font_size=50, max_words=100, background_color="white").generate_from_frequencies(
            word_freq)
        plt.figure()
        plt.imshow(wordcloud, interpolation="bilinear")
        plt.axis("off")

        lwordcloud = WordCloud(max_font_size=50, max_words=100, background_color="white").generate_from_frequencies(
            lword_freq)
        plt.figure()
        plt.imshow(lwordcloud, interpolation="bilinear")
        plt.axis("off")

        hwordcloud = WordCloud(max_font_size=50, max_words=100, background_color="white").generate_from_frequencies(
            hash_freq)
        plt.figure()
        plt.imshow(hwordcloud, interpolation="bilinear")
        plt.axis("off")

        plt.show()


def users_bio_sentiment_analysis():
    with open('users_bio.csv', mode='r', encoding='utf8') as fin_users, open(
            'words.txt', mode='r', encoding='utf8') as fin_words, open(
        'sentiments.txt', mode='w') as fout_sent:
        # users = fin_users.readlines()
        bios = fin_words.readlines()
        sia = SentimentIntensityAnalyzer()
        classifier = TextClassifier.load("en-sentiment")
        labels = set()
        fout_sent.write(f'Sl\tVader-Overall\tNegative\tNeutral\tPositive\tCompound\tFlair-Overall\tPercentage\n')
        for i in range(0, len(bios), 1):
            bio = bios[i].strip()
            if len(bio) == 0:
                continue
            # user = users[i].split('\t')[0]

            try:
                sent_dict = sia.polarity_scores(bio)
                overall_score = "Neutral"
                if sent_dict["compound"] >= 0.05:
                    overall_score = "Positive"
                elif sent_dict["compound"] <= -0.05:
                    overall_score = "Negative"

                sentence = Sentence(bio)
                classifier.predict(sentence)
                temp = ''
                if len(sentence.labels) > 1:
                    print(f"{bio}, {sentence.labels}")

                temp = str(sentence.labels[0]).split(" ")
                labels.add(temp[0])

                fout_sent.write(
                    f'{i}\t{overall_score}\t{sent_dict["neg"]}\t{sent_dict["neu"]}\t{sent_dict["pos"]}\t{sent_dict["compound"]}\t{temp[0]}\t{temp[1][1:-1]}\n')

            except Exception as e:
                traceback.print_exc()
                print(bio)

        print(labels)


def users_bio_sentiment_pie():
    vader_pos = vader_neg = vader_neu = 0
    flair_pos = flair_neg = 0
    vp_fp = vp_fn = vn_fp = vn_fn = vnu_fp = vnu_fn = 0

    with open("sentiments.txt", mode="r") as fin_sent:
        for line in fin_sent:
            temp = line.strip().split("\t")
            overall = f'{temp[1]}{temp[-2]}'

            if overall == "PositivePOSITIVE":
                vader_pos += 1
                flair_pos += 1
                vp_fp += 1
            elif overall == "PositiveNEGATIVE":
                vader_pos += 1
                flair_neg += 1
                vp_fn += 1
            elif overall == "NegativePOSITIVE":
                vader_neg += 1
                flair_pos += 1
                vn_fp += 1
            elif overall == "NegativeNEGATIVE":
                vader_neg += 1
                flair_neg += 1
                vn_fn += 1
            elif overall == "NeutralPOSITIVE":
                vader_neu += 1
                flair_pos += 1
                vnu_fp += 1
            elif overall == "NeutralNEGATIVE":
                vader_neu += 1
                flair_neg += 1
                vnu_fn += 1

    print(f"Vader: {vader_pos}, {vader_neg}, {vader_neu}")
    print(f"Flair: {flair_pos}, {flair_neg}")
    print(f"Vader Pos: {vp_fp}\t{vp_fn}")
    print(f"Vader Neg; {vn_fp}\t{vn_fn}")
    print(f"Vader Neu; {vnu_fp}\t{vnu_fn}")

    total = (vader_pos + vader_neg + vader_neu)
    y = [vader_pos, vader_neg, vader_neu]
    labels = [f"Positive {(vader_pos * 100 / total):.2f}%",
              f"Negative {(vader_neg * 100 / total):.2f}%",
              f"Neutral {(vader_neu * 100 / total):.2f}%"]
    colors = ["#488f31", "#de425b", "#ffe692"]
    explode = [0, 0.1, 0]
    plt.pie(y, labels=labels, colors=colors, explode=explode, shadow=True)

    plt.show()

    total = (flair_pos + flair_neg)
    y = [flair_pos, flair_neg]
    labels = [f"Positive {(flair_pos * 100 / total):.2f}%",
              f"Negative {(flair_neg * 100 / total):.2f}%"]
    colors = ["#488f31", "#de425b"]
    explode = [0, 0]
    plt.pie(y, labels=labels, colors=colors, explode=explode, shadow=True)

    plt.show()


def users_bio_ed_distilbert():
    ed_distilbert('words_with_emoji.txt', 'users_bio_distilbert.csv')


def tweet_ed_distilbert():
    ed_distilbert('../data/tweet_words_with_emoji_remaining.tsv', '../data/tweet_distilbert.csv')


def ed_distilbert(in_file, out_file):
    """
    using https://huggingface.co/bhadresh-savani/distilbert-base-uncased-emotion
    with the emojis but removing hashtags, mentions, and urls.
    """

    classifier = pipeline("text-classification", model='bhadresh-savani/distilbert-base-uncased-emotion',
                          return_all_scores=True)
    with open(in_file, mode='r', encoding='utf8') as fin, open(out_file, mode='a', encoding='utf8') as fout:

        # fout.write('id,sadness,joy,love,anger,fear,surprise,verdict\n')

        index = 0
        current_line = 0
        for line in fin:
            if current_line < index:
                current_line += 1
                continue

            try:
                index += 1
                parts = line.strip().split('\t')

                if len(parts) == 1:
                    continue

                prediction = classifier(parts[1], )

                # I'm not confident if the classifier will return the dict with keys in the same order
                # Also to get the verdict using maximum probability need to check every score

                sadness = joy = love = anger = fear = surprise = 0
                verdict = ''
                max_prob = 0
                for pred in prediction[0]:
                    if pred['label'] == 'sadness':
                        sadness = pred['score']
                        if sadness > max_prob:
                            max_prob = sadness
                            verdict = 'sadness'
                    elif pred['label'] == 'joy':
                        joy = pred['score']
                        if joy > max_prob:
                            max_prob = joy
                            verdict = 'joy'
                    elif pred['label'] == 'love':
                        love = pred['score']
                        if love > max_prob:
                            max_prob = love
                            verdict = 'love'
                    elif pred['label'] == 'anger':
                        anger = pred['score']
                        if anger > max_prob:
                            max_prob = anger
                            verdict = 'anger'
                    elif pred['label'] == 'fear':
                        fear = pred['score']
                        if fear > max_prob:
                            max_prob = fear
                            verdict = 'fear'
                    elif pred['label'] == 'surprise':
                        surprise = pred['score']
                        if surprise > max_prob:
                            max_prob = surprise
                            verdict = 'surprise'

                fout.write(f'{parts[0]},{sadness},{joy},{love},{anger},{fear},{surprise},{verdict}\n')

            except Exception as e:
                print(e)
                print(line)
                exit(index)


def users_bio_ed_distilbert_goemotions():
    """
    using https://huggingface.co/joeddav/distilbert-base-uncased-go-emotions-student
    student of models trained on GoEmotions dataset with 27 emotions

    [[{"label":"admiration","score":0.03721676021814346},
    {"label":"amusement","score":0.01585017889738083},
    {"label":"anger","score":0.003236852353438735},
    {"label":"annoyance","score":0.003161841770634055},
    {"label":"approval","score":0.07673002034425736},
    {"label":"caring","score":0.06162014231085777},
    {"label":"confusion","score":0.004896118305623531},
    {"label":"curiosity","score":0.021125035360455514},
    {"label":"desire","score":0.03356267139315605},
    {"label":"disappointment","score":0.0011842024978250266},
    {"label":"disapproval","score":0.0027064187452197077},
    {"label":"disgust","score":0.002020828425884247},
    {"label":"embarrassment","score":0.0022175440099090339},
    {"label":"excitement","score":0.051230840384960178},
    {"label":"fear","score":0.0027683808002620937},
    {"label":"gratitude","score":0.13742570579051972},
    {"label":"grief","score":0.0030143226031214},
    {"label":"joy","score":0.06514319777488709},
    {"label":"love","score":0.014916658401489258},
    {"label":"nervousness","score":0.006636080797761679},
    {"label":"optimism","score":0.09592127799987793},
    {"label":"pride","score":0.06627731025218964},
    {"label":"realization","score":0.06447335332632065},
    {"label":"relief","score":0.17632371187210084},
    {"label":"remorse","score":0.006228619255125523},
    {"label":"sadness","score":0.001663982286117971},
    {"label":"surprise","score":0.028542600572109224},
    {"label":"neutral","score":0.013905340805649758}]]
    """

    tokenizer = AutoTokenizer.from_pretrained("joeddav/distilbert-base-uncased-go-emotions-student")
    # model = AutoModelForSequenceClassification.from_pretrained("joeddav/distilbert-base-uncased-go-emotions-student")
    classifier = pipeline("text-classification", model='joeddav/distilbert-base-uncased-go-emotions-student',
                          return_all_scores=True)
    with open('words_with_emoji.txt', mode='r', encoding='utf8') as fin, open(
            'users_bio_distilbert_goemotions.csv', mode='a') as fout:

        fout.write('id,admiration,amusement,anger,annoyance,approval,' + \
                   'caring,confusion,curiosity,desire,disappointment,disapproval,' + \
                   'disgust,embarrassment,excitement,fear,gratitude,grief,joy,love,' + \
                   'nervousness,optimism,pride,realization,relief,remorse,sadness,surprise,neutral,verdict\n')
        lines = fin.readlines()
        index = 0
        total_lines = len(lines)
        while index < total_lines:
            line = lines[index]

            try:
                index += 1
                parts = line.strip().split('\t')

                if len(parts) == 1:
                    continue

                prediction = classifier(parts[1], )

                verdict = ''
                max_prob = 0
                line = ','
                for pred in prediction[0]:
                    line += pred['score'] + ','
                    if max_prob < pred['score']:
                        max_prob = pred['score']
                        verdict = pred['label']

                fout.write(f'{parts[0]}{line}{verdict}\n')

            except Exception as e:
                print(e)
                print(line)


def user_bio_graph_distilbert():
    sadness = joy = love = anger = fear = surprise = 0
    with open('users_bio_distilbert.csv', mode='r') as fin:
        for line in fin:
            parts = line.strip().split(',')
            verdict = parts[7]

            if verdict == 'sadness':
                sadness += 1

            elif verdict == 'joy':
                joy += 1

            elif verdict == 'love':
                love += 1

            elif verdict == 'anger':
                anger += 1

            elif verdict == 'fear':
                fear += 1

            elif verdict == 'surprise':
                surprise += 1

    plt.bar


def bar_chart():
    with open('..\\data\\users_bio_distilbert.csv', mode='r') as fin:
        lines = fin.readlines()
        counts = defaultdict(dict)
        cols = lines[0].strip().split(',')[1:]

        for i in range(1, len(lines), 1):
            parts = lines[i].strip().split(',')[1:7]
            for j in range(len(parts)):
                val = float(parts[j])
                if 'below' not in counts[cols[j]]:
                    counts[cols[j]]['below'] = 0
                    counts[cols[j]]['above'] = 0

                if val < 0.5:
                    counts[cols[j]]['below'] -= 1
                else:
                    counts[cols[j]]['above'] += 1

        print(counts)

        df = pd.DataFrame(counts)
        df = df.transpose()
        df.plot.bar(stacked=True)
        plt.show()


def main():
    # data_plotter("../data/all_following.c sv", 2)
    # users_bio_word_cloud()
    # users_bio_sentiment_analysis()
    # users_bio_sentiment_pie()
    # users_bio_ed_distilbert()
    # bar_chart()
    tweet_ed_distilbert()


if __name__ == '__main__':
    main()
    print('done')
