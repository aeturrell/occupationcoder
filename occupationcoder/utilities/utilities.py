#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Dec  6 11:58:38 2017

@author: jdjumalieva
"""
import nltk
import re
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer
import os
import json
import pandas as pd

script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
lookup_dir = os.path.join(parent_dir, 'dictionaries')


with open(os.path.join(lookup_dir, 'known_words_dict.json'), 'r') as infile:
    known_words_dict = json.load(infile)

with open(os.path.join(lookup_dir, 'expand_dict.json'), 'r') as infile:
    expand_dict = json.load(infile)

with open(os.path.join(lookup_dir, 'titles_minor_group_ons.json'), 'r') as infile:
    titles_mg = json.load(infile)

mg_buckets = pd.read_json(os.path.join(lookup_dir,'mg_buckets_ons_df_processed.json'))



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
    keep_asis = ['sales', 'years', 'goods', 'operations', 'systems',
                    'communications', 'events', 'loans', 'grounds',
                    'lettings', 'claims', 'accounts', 'relations',
                    'complaints', 'services']
    wnl = nltk.WordNetLemmatizer()
    processed_terms = [wnl.lemmatize(i) if i not in keep_asis else i for i in title_terms]
    return processed_terms


def replace_word(word, lookup_dict):
    """
    Takes string and a look up dictionary as input.
    The input string is used as a key in a dictionary. Function returns a value
    in a string format for the specified key in a provided dictionary.

    >>> replace_word('rgn', expand_dict)
    u'registered general nurse'
    """
    word=lookup_dict[word]
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
    words = [replace_word(word, this_dict) if word in keys else word for word in words]
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


select_punct = set('!"#$%&\()*+,-./:;<=>?@[\\]^_`{|}~') #only removed "'"
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


from io import StringIO
from html.parser import HTMLParser

# class MLStripper(HTMLParser):
#     def __init__(self):
#         super().__init__()
#         self.reset()
#         self.strict = False
#         self.convert_charrefs= True
#         self.text = StringIO()
#     def handle_data(self, d):
#         self.text.write(d)
#     def get_data(self):
#         return self.text.getvalue()

# def strip_tags(html):
#     """
#     Takes string as input.
#     Removes html tags.
#     Returns a string.
#     """
#     s = MLStripper()
#     s.feed(html)
#     return s.get_data()

from bs4 import BeautifulSoup
def strip_tags(html):
    """
    Takes string as input.
    Removes html tags.
    Returns a string.
    """
    soup = BeautifulSoup(html, features="html.parser")
    return soup.get_text()


def clean_title(dataframe_row):
    """
    Takes string in a dataframe column 'job_title' as input. In sequence applies functions to lemmatise terms,
    expand abbreviations, replace words not in ONS classification, remove digits,
    remove punctuation and extra spaces. This function is adapted to work with
    dataframe fields and not individual strings.
    Returns a string.

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
    lower = strip_tags(dataframe_row['job_title']).lower()
    lemm = lemmatise(lower.split())
    exp = lookup_replacement(lemm, expand_dict)
    known = replace_unknown(' '.join(exp)).strip()
    nodigits = remove_digits(known)
    nopunct = replace_punctuation(nodigits)
    nospace = re.sub(' +',' ',nopunct)
    nospace = nospace.strip()
    return nospace

def clean_desc(dataframe_row):
    """
    Takes string in a dataframe field 'job_description' as input. In sequence
    applies functions to lemmatise terms, expand abbreviations, remove punctuaiton
    and extra spaces. This function is adapted to work with dataframe fields
    and not individual strings.
    Returns a string.

    """
    lower = strip_tags(dataframe_row['job_description']).lower()
    lemm = lemmatise(lower.split())
    exp = lookup_replacement(lemm, expand_dict)
    nopunct = replace_punctuation(' '.join(exp))
    nospace = re.sub(' +',' ',nopunct)
    nospace = nospace.strip()
    return nospace

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
    lower = replace_punctuation(dataframe_row['job_sector'].lower())
    replaced = lower.replace('other', ' ')
    exp = lookup_replacement(replaced.split(), expand_dict)
    nospace = re.sub(' +',' ',' '.join(exp))
    nospace = nospace.strip()
    return nospace


def exact_match(some_title):
    """
    Takes string as input.
    Checks if the first three words in a job title match exactly any known
    titles in the titles_mg dict.
    Returns matching SOC code if there is an exact match or 'NA' if not.

    >>> exact_match('medical writer')
    '341'

    >>> exact_match('professional medical writer')
    'NA'
    """
    title = ' '.join(some_title.split()[:3])
    result = 'NA'
    keys = titles_mg.keys()
    for k in keys:
        for i in titles_mg[k]:
            if i == title:
                result = k
            else:
                continue
    return result


def tokenize(text):
    """
    Takes string as input.
    Returns list of tokens. The function is used as an argument for
    TfidfVectorizer.

    >>> tokenize('some job title')
    ['some', 'job', 'title']
    """
    tokens = nltk.word_tokenize(text)
    return tokens


def get_best_score_top5_2(dataframe_row):
    """
    Takes string as input.
    Generates tf-idf for a raw title, then calculates cosine similarity to each
    of 90 ONS minor group title and description buckets and picks 5 buckets
    with the highest cosine similarity.
    Returns a list of SOC_codes for the minor groups (in ascending order by
    cosine similarity).

    >>> from sklearn.feature_extraction.text import TfidfVectorizer
    >>> from sklearn.metrics.pairwise import cosine_similarity
    >>> textfortoken= mg_buckets.Titles_nospace
    >>> tfidf = TfidfVectorizer(tokenizer=tokenize, stop_words='english',ngram_range=(1,3))
    >>> tfidfm = tfidf.fit_transform(textfortoken)
    >>> get_best_score_top5_2('community nurse')
    ['331', '612', '323', '614', '223']
    """
    textfortoken= mg_buckets.Titles_nospace
    tfidf = TfidfVectorizer(tokenizer=tokenize,
                                stop_words='english',
                                ngram_range=(1,3))
    tfidfm = tfidf.fit_transform(textfortoken)
    new_i = tfidf.transform([dataframe_row])
    known_titles = tfidfm
    cosine_similarities = cosine_similarity(new_i, known_titles)
    best = cosine_similarities.argsort()[0,-5:]
    result = []
    append = result.append
    for b in best:
        append(mg_buckets.SOC_code[b])
    return result


def getKey(item):
    """
    Takes any iterable as input.
    Returns tuple.
    Is used to specify order of argument importance, which can be used for
    sorting and gettin max using multiple criteria.

    >>> getKey(('registered nurse', 90, 4))
    (90, 4, 2)
    """
    return (item[1], item[2], len(item[0].split()))


def getKey2(item):
    """
    Takes any iterable as input.
    Returns tuple.
    Is used to specify order of importance, which can be used for sort and max.
    """
    return (item[1][0], item[2])


def extract(some_string, some_list):
    """
    Takes a string and a list as input. Uses fuzzywuzzy process.extractOne
    method to extract the item with the highest fuzzy match to input string
    from the provided list.

    >>> extract('recruitment consultant', ['recruitment coordinator',
    'director of recruitment', 'management consultant'])
    ('recruitment coordinator', 76)
    """
    from fuzzywuzzy import process
    opt = process.extractOne(str(some_string), some_list)
    return opt


def return_best_match_2(dataframe_row):
    """
    Takes dataframe row as input.
    Finds the match for 'title_nospace' field with the highest fuzzy score out
    of all known titles in the top 5 ONS minor group buckets stored in 'top5'.
    This function is adapted to work with dataframe fields and not individual
    strings.
    Returns a tuple.

    >>> sample_df.loc[95:, ['title_nospace', 'job_sector_nospace']]
                                   title_nospace       job_sector_nospace
    95                    recruitment consultant  recruitment consultancy
    96                     care assistant worker              social care
    97  field marketing and business development             marketing pr
    98                          personal trainer                 training
    99                     care assistant worker              social care

    >>> sample_df['title_nospace'] = sample_df.apply(lambda x:\
    clean_title(x), axis=1)
    >>> sample_df['desc_nospace'] = sample_df.apply(lambda x:\
    clean_desc(x), axis=1)
    >>> sample_df['job_sector_nospace'] = sample_df.apply(lambda x:\
    clean_sector(x), axis=1)
    >>> sample_df['title_and_desc'] = sample_df[['title_nospace',\
    'job_sector_nospace','desc_nospace']].apply(lambda x: ' '.join(x), axis=1)
    >>> sample_df.loc[:,'top5'] = sample_df['title_and_desc'].\
    map(get_best_score_top5_2)
    >>> res = sample_df.apply(return_best_match_2, axis=1)
    >>> res.tail()
    95      (356, (recruitment consultant, 100, 1))
    96               (614, (care assistant, 90, 4))
    97    (354, (business sales executives, 86, 4))
    98            (344, (personal trainer, 100, 4))
    99               (614, (care assistant, 90, 4))
    dtype: object
    """
    codes = dataframe_row['top5']
    index = codes.index
    options ={}
    items = options.items
    for c in codes:
        if dataframe_row['title_nospace'] != '':
            good_opt = extract(str(dataframe_row['title_nospace']), titles_mg[c])
            if good_opt[1] == 0:
                options[c] = ('None', '0')
            else:
                options[c]=(good_opt[0], good_opt[1], index(c))
            final_code = max(items(), key = lambda x: getKey(x[1]))
        else:
            final_code = (codes[4], ('None'))
    return final_code

def ascii_convert(cols,dfIn):
    """
    Function cleans ascii input and converts everything to strings in unicode
    """
    for col in cols:
        dfIn[col] = dfIn[col].astype(unicode)
        dfIn[col] = dfIn[col].apply(lambda x: x.encode('ascii', 'replace'))
        dfIn[col] = dfIn[col].astype(str)
    return dfIn
