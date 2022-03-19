from googleapiclient import discovery
import json

API_KEY = 'AIzaSyDXnpQT0XpYwN4RMj4rOkXRCnkwAFY17Tg'

client = discovery.build(
    "commentanalyzer",
    "v1alpha1",
    developerKey=API_KEY,
    discoveryServiceUrl="https://commentanalyzer.googleapis.com/$discovery/rest?version=v1alpha1",
    static_discovery=False,
)

analyze_request = {
    'comment': {
        'text': "we believe in the power of social media protest and boycotts to bring social justice and change we report news other gay news media will not"
    },
    'requestedAttributes': {
        'TOXICITY': {},
        'SEVERE_TOXICITY': {},
        'IDENTITY_ATTACK': {},
        'INSULT': {},
        'PROFANITY': {},
        'THREAT': {}
    },
    'doNotStore': True
}

response = client.comments().analyze(body=analyze_request).execute()
for label in response['attributeScores']:
    print(f"{label}: {response['attributeScores'][label]['summaryScore']['value']}")
# print(json.dumps(response, indent=2))
