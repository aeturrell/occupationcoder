import nltk
import re
import os
import json

from nltk.corpus import stopwords
STOPWORDS = stopwords.words('english')

script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
lookup_dir = os.path.join(parent_dir, 'dictionaries')

# If we put this here we only have to instantiate it once...
wnl = nltk.WordNetLemmatizer()

# List of terms we want to NOT lemmatize for some reason
KEEP_AS_IS = ['sales', 'years', 'goods', 'operations', 'systems',
              'communications', 'events', 'loans', 'grounds',
              'lettings', 'claims', 'accounts', 'relations',
              'complaints', 'services']

with open(os.path.join(lookup_dir, 'known_words_dict.json'), 'r') as infile:
    known_words_dict = json.load(infile)

with open(os.path.join(lookup_dir, 'expand_dict.json'), 'r') as infile:
    expand_dict = json.load(infile)


def simple_clean(text: str, known_only=True):
    """
    Takes string as input, cleans, lowercases, tokenizes and lemmatises,
    before replacing some known tokens with synonyms and optionally filtering
    to only known (job title) words.

    Keyword arguments:
        text -- String representing human freetext to clean up
        known_only -- Bool, whether to filter to only known job title words (default True)
    """
    # Handle unexpected datatypes
    if type(text) != str:
        raise TypeError("simple_clean expects a string")

    text = re.sub(r'<.*?>', " ", text)  # Clean out any HTML tags
    text = re.sub(r"[^a-z ]", " ", text.lower())  # Keep only letters and spaces, lowercase
    text = re.sub(' +', ' ', text).strip()   # Remove excess whitespace

    # Lemmatise tokens
    tokens = [wnl.lemmatize(token) if token not in KEEP_AS_IS else token
              for token in text.split()]

    # Replace some lemmas with known synonyms, if known
    tokens = [expand_dict.get(token, token) for token in tokens]

    # Filter out words not present in the vocabulary we're matching to
    if known_only:
        tokens = [token for token in tokens if token in list(known_words_dict.keys())]

    # Filter out stopwords
    tokens = [token for token in tokens if token not in STOPWORDS]

    return " ".join(tokens)
