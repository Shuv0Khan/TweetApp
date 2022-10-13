import time
import traceback
from pathlib import Path
import pandas as pd
import regex as re
from googleapiclient import discovery
from tqdm.auto import tqdm

# IN_FILE_PATH = '../data/user_bio_processed_for_perspective.tsv'
# OUT_FILE_PATH = '../data/users_bio_perspective.csv'

IN_FILE_PATH = '../data/tweets_neg_sample_perspective_api.tsv'
OUT_FILE_PATH = '../data/tweets_neg_sample_perspective.csv'

API_KEY = 'REDACTED'

client = discovery.build(
    "commentanalyzer",
    "v1alpha1",
    developerKey=API_KEY,
    discoveryServiceUrl="https://commentanalyzer.googleapis.com/$discovery/rest?version=v1alpha1",
    static_discovery=False,
)

analyze_request = {
    'comment': {
        'text': ""
    },
    'requestedAttributes': {
        'TOXICITY': {},
        'SEVERE_TOXICITY': {},
        'IDENTITY_ATTACK': {},
        'INSULT': {},
        'PROFANITY': {},
        'THREAT': {}
    },
    'doNotStore': True,
    'languages': ['en']
}


def get_perspective(line: str) -> dict:
    analyze_request['comment']['text'] = line
    return client.comments().analyze(body=analyze_request).execute()


def run():
    in_path = Path(IN_FILE_PATH)
    if not in_path.exists() or not in_path.is_file():
        raise Exception("Path error. File not found")

    df = pd.read_csv(in_path, sep='\t', header=0,
                     # dtype={"id": "string", "month": "string", "tweet": "string"}
                     )
    df['TOXICITY'] = ''
    df['SEVERE_TOXICITY'] = ''
    df['IDENTITY_ATTACK'] = ''
    df['INSULT'] = ''
    df['PROFANITY'] = ''
    df['THREAT'] = ''

    print(df.info())

    out_cols = df.columns.tolist()
    out_cols.remove('tweet')

    out_path = Path(OUT_FILE_PATH)
    start_index = 0
    print_headers = False

    if out_path.is_file():
        with open(out_path, mode='r') as fin:
            start_index = len(fin.readlines()) - 1
    else:
        print_headers = True

    with open(OUT_FILE_PATH, mode='a') as fout:
        if print_headers:
            fout.write(f"{str(out_cols)[1:-1]}\n")

        for i in tqdm(range(start_index, len(df))):
            text = df.loc[i]['tweet']

            labeled = False
            while not labeled:
                try:
                    probs = get_perspective(text)
                    for label in probs['attributeScores']:
                        df.loc[i, label] = probs['attributeScores'][label]['summaryScore']['value']

                    labeled = True
                except Exception:
                    traceback.print_exc()
                    time.sleep(.5)

            # Written line by line to avoid data loss on restarts
            s = str(df.loc[i, out_cols].to_list())
            s = re.sub(r"[\[\]\']", '', s)
            fout.write(f"{s}\n")

            # 1 req per second
            time.sleep(1)


if __name__ == '__main__':
    run()
