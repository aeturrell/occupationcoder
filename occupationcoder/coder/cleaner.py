from bs4 import BeautifulSoup
import nltk
import re
import os
import json
import pandas as pd

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
    to only known words.
    """
    # Handle unexpected datatypes
    if type(text) != str:
        raise TypeError("simple_clean expects a string")

    text = re.sub(r'<.*?>', " ", text)  # Clean out any HTML tags
    text = re.sub(r"[^a-z ]", " ", text.lower())  # Keep only letters and spaces, lowercase
    text = re.sub(' +', ' ', text).strip()   # Remove excess whitespace

    # Lemmatize tokens
    tokens = [wnl.lemmatize(token) if token not in KEEP_AS_IS else token
              for token in text.split()]

    # Replace some lemmas with known synonyms, if known
    tokens = [expand_dict.get(token, token) for token in tokens]

    # Filter out words not present in the vocabulary we're matching to
    if known_only:
        tokens = [token for token in tokens if token in list(known_words_dict.keys())]

    return " ".join(tokens)


def lemmatise(title_terms):
    """
    Takes list as input.
    Removes suffixes if the new words exists in the nltk dictionary.
    The purpose of the function is to convert plural forms into singular.
    Allows some nouns to remain in plural form (the to_keep_asis is manually curated).
    Returns a list.

    >>> lemmatise(['teachers'])
    u'teacher']

    >>> lemmatise(['analytics'])
    ['analytics']
    """

    wnl = nltk.WordNetLemmatizer()
    processed_terms = [wnl.lemmatize(
        i) if i not in KEEP_AS_IS else i for i in title_terms]
    return processed_terms


def replace_word(word, lookup_dict):
    """
    Takes string and a look up dictionary as input.
    The input string is used as a key in a dictionary. Function returns a value
    in a string format for the specified key in a provided dictionary.

    >>> replace_word('rgn', expand_dict)
    u'registered general nurse'
    """
    word = lookup_dict[word]
    return word


def lookup_replacement(words, lookup_dict):
    """
    Takes a list and a dict as input, replaces words in the list if they exist
    in the dict as keys with corresponding values .
    Returns a list.
    This function is used to expand abbreviations.

    >>> lookup_replacement(['pa', 'to', 'vice', 'president'], expand_dict)
    [u'personal assistant', 'to', 'vice', 'president']
    """
    keys = lookup_dict.keys()
    this_dict = lookup_dict
    words = [replace_word(word, this_dict)
             if word in keys else word for word in words]
    return words


def replace_unknown(s):
    """
    Takes a string and a ONS known words dict as input.
    Only keeps the words that exist in ONS title words.
    Returns a string.

    >>> replace_unknown('high court enforcement agent london x2')
    'high court enforcement agent'
    """
    all_words = s.split()
    tokeep = []
    keys = known_words_dict.keys()
    tokeep = [i for i in all_words if i in keys]
    clean_words = ' '.join(tokeep)
    return clean_words


def remove_digits(s):
    """
    Takes a string as input.
    Removes digits in a string.
    Returns a string.

    >>> remove_digits('2 recruitment consultants')
    ' recruitment consultants'
    """
    result = ''.join(i for i in s if not i.isdigit())
    return result


select_punct = set('!"#$%&\()*+,-./:;<=>?@[\\]^_`{|}~')  # only removed "'"


def replace_punctuation(s):
    """
    Takes string as input.
    Removes punctuation from a string if the character is in select_punct.
    Returns a string.

   >>> replace_punctuation('sales executives/ - london')
   'sales executives   london'
    """
    for i in set(select_punct):
        if i in s:
            s = s.replace(i, ' ')
    return s


def strip_tags(html):
    """
    Takes string as input.
    Removes html tags.
    Returns a string.
    """
    soup = BeautifulSoup(html, features="html.parser")
    return soup.get_text()


def clean_title(df_row):
    """
    Applies simple clean, see docs on simple clean

    >>> sample_df['job_title']
    0             recruitment consultant  be your own boss
    1    financial consultant£4284633462 plus bonus an...
    2    dutch speaking contact services representative...
    3                               care assistant  worker
    4                               care assistant  worker
    Name: job_title, dtype: object

    >>> sample_df.apply(lambda x: clean_title(x), axis=1)
    0             recruitment consultant
    1       financial plus bonus and car
    2    contact services representative
    3              care assistant worker
    4              care assistant worker
    dtype: object
    """
    text = df_row['job_title']
    return simple_clean(text)


def clean_desc(dataframe_row):
    text = dataframe_row['job_description']
    return simple_clean(text, known_only=False)


def clean_sector(dataframe_row):
    """
    Takes string in a dataframe field 'job_sector' as input. In sequence
    applies functions to collapse case, replace 'other' with ' ', expand
    abbreviations, remove punctuaiton and extra spaces.
    This function is adapted to work with dataframe fields and not individual
    strings.
    Returns a string.

    >>> sample_df['job_sector']

    93                Social Care
    94     Hospitality & Catering
    95    Recruitment Consultancy
    96                Social Care
    97             Marketing & PR
    98                   Training
    Name: job_sector, dtype: object

    >>> sample_df.apply(lambda x: clean_sector(x), axis=1)

    93                social care
    94       hospitality catering
    95    recruitment consultancy
    96                social care
    97               marketing pr
    98                   training
    dtype: object
    """
    text = dataframe_row['job_sector']
    return simple_clean(text, known_only=False)
