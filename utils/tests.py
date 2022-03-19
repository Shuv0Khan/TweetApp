import emoji
import nltk
import regex as re

stop_words = set(nltk.corpus.stopwords.words('english'))

contraction_mapping = {"ain't": "is not", "aren't": "are not","can't": "cannot", "'cause": "because", "could've": "could have", "couldn't": "could not",
                       "didn't": "did not",  "doesn't": "does not", "don't": "do not", "hadn't": "had not", "hasn't": "has not", "haven't": "have not",
                       "he'd": "he would","he'll": "he will", "he's": "he is", "how'd": "how did", "how'd'y": "how do you", "how'll": "how will",
                       "how's": "how is",  "I'd": "I would", "I'd've": "I would have", "I'll": "I will", "I'll've": "I will have","I'm": "I am",
                       "I've": "I have", "i'd": "i would", "i'd've": "i would have", "i'll": "i will",  "i'll've": "i will have","i'm": "i am",
                       "i've": "i have", "isn't": "is not", "it'd": "it would", "it'd've": "it would have", "it'll": "it will", "it'll've": "it will have",
                       "it's": "it is", "let's": "let us", "ma'am": "madam", "mayn't": "may not", "might've": "might have","mightn't": "might not",
                       "mightn't've": "might not have", "must've": "must have", "mustn't": "must not", "mustn't've": "must not have", "needn't": "need not",
                       "needn't've": "need not have","o'clock": "of the clock", "oughtn't": "ought not", "oughtn't've": "ought not have", "shan't": "shall not",
                       "sha'n't": "shall not", "shan't've": "shall not have", "she'd": "she would", "she'd've": "she would have", "she'll": "she will",
                       "she'll've": "she will have", "she's": "she is", "should've": "should have", "shouldn't": "should not", "shouldn't've": "should not have",
                       "so've": "so have","so's": "so as", "this's": "this is","that'd": "that would", "that'd've": "that would have", "that's": "that is",
                       "there'd": "there would", "there'd've": "there would have", "there's": "there is", "here's": "here is","they'd": "they would",
                       "they'd've": "they would have", "they'll": "they will", "they'll've": "they will have", "they're": "they are", "they've": "they have",
                       "to've": "to have", "wasn't": "was not", "we'd": "we would", "we'd've": "we would have", "we'll": "we will", "we'll've": "we will have",
                       "we're": "we are", "we've": "we have", "weren't": "were not", "what'll": "what will", "what'll've": "what will have",
                       "what're": "what are",  "what's": "what is", "what've": "what have", "when's": "when is", "when've": "when have", "where'd": "where did",
                       "where's": "where is", "where've": "where have", "who'll": "who will", "who'll've": "who will have", "who's": "who is",
                       "who've": "who have", "why's": "why is", "why've": "why have", "will've": "will have", "won't": "will not", "won't've": "will not have",
                       "would've": "would have", "wouldn't": "would not", "wouldn't've": "would not have", "y'all": "you all", "y'all'd": "you all would",
                       "y'all'd've": "you all would have","y'all're": "you all are","y'all've": "you all have","you'd": "you would", "you'd've": "you would have",
                       "you'll": "you will", "you'll've": "you will have", "you're": "you are", "you've": "you have", 'u.s':'america', 'e.g':'for example'}

# Regex patterns.
urlPattern             = r"(quick\s*link[s*]\s*:\s*)*((http://)[^ ]*|(https://)[^ ]*|( www\.)[^ ]*)"
userPattern            = r'@[^\s]+'
hashtagPattern         = r'#[^\s]+'
specialQuotesPattern   = r'[’|‘|´|`]+'
alphaPattern           = r"[^a-zA-Z0-9'\"\.\,\?\!\&\%\$\/\-+]"
validPuncPattern       = r"([\"\.\,\?\!\&\%\$\/\-+])"
breakAlphaNumPattern   = r'([0-9]+)'
sequencePattern        = r"(.)\1\1+"
seqReplacePattern      = r"\1\1"


def process_text(s):
    # Remove links
    s = re.sub(urlPattern, ' ', s)

    # Replace tabs with whitespace
    s = s.replace('\t', ' ')

    # Replace all emojis.
    s = emoji.replace_emoji(s, ' ')

    # Replace @USERNAME to ' '.
    s = re.sub(userPattern, ' ', s)

    # Replace #HASHTAG to ' '
    s = re.sub(hashtagPattern, ' ', s)

    # Replace special quotes
    s = re.sub(specialQuotesPattern, "'", s)

    # Replace 1 or more of valid punctuations with 1
    s = re.sub(validPuncPattern + r"\1+", r"\1", s)

    # Replace 3 or more consecutive letters by 2 letter.
    s = re.sub(sequencePattern, seqReplacePattern, s)

    # Replace all non-english alphabets, digits and invalid punctuations.
    s = re.sub(alphaPattern, " ", s)

    # Put space between punctuations and letters/digits
    s = re.sub(validPuncPattern, r" \1 ", s)

    # Break alphanumeric into words and numbers
    s = re.sub(breakAlphaNumPattern, r' \1 ', s)

    # If no alphabet/digits remain
    if re.match(r'.*[a-zA-Z0-9]+.*', s) is None:
        return ''

    # Tokenize
    # tokens = nltk.word_tokenize(s) # breaks contractions into 2 words
    tokens = s.split()

    valid_bag = []
    for w in tokens:
        # Remove stopwords
        # if w in stop_words:
        #     continue
        # Contraction Mapping
        if w in contraction_mapping:
            valid_bag.extend(contraction_mapping[w].split())

        valid_bag.append(w)

    # At least 2 words or 1 word and 1 punctuation
    if len(valid_bag) < 2:
        return ''

    return ' '.join(valid_bag)



list = ['#paisleybuddie @❤️luv my5brats nssrp 👵🏼❤️@irvinewelsh. ❤️. @sarahpinborough 📖🎬 #imagranny ❤️👵🏼❤️ #histitch ❤️💙🐾',
        "we are the organizers of regina's queen city pride festival! pride 2021 is from june 4 to 13!  quick links: https://t.co/ae11smu5mj  #qcpride",
        "Meninsn?? Yessss please!!!!!"]

if __name__=='__main__':
    print(process_text(list[0]))
    print(process_text(list[1]))
    print(process_text(list[2]))