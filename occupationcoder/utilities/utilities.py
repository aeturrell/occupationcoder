#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Dec  6 11:58:38 2017

@author: jdjumalieva
"""
import nltk
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer
import os
import json
import pandas as pd
from fuzzywuzzy import process

from ..coder.cleaner import clean_title, clean_desc, clean_sector

script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
lookup_dir = os.path.join(parent_dir, 'dictionaries')

with open(os.path.join(lookup_dir, 'titles_minor_group_ons.json'), 'r') as infile:
    titles_mg = json.load(infile)

mg_buckets = pd.read_json(os.path.join(
    lookup_dir, 'mg_buckets_ons_df_processed.json'))


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
    textfortoken = mg_buckets.Titles_nospace
    tfidf = TfidfVectorizer(tokenizer=tokenize,
                            stop_words='english',
                            ngram_range=(1, 3))
    tfidfm = tfidf.fit_transform(textfortoken)
    new_i = tfidf.transform([dataframe_row])
    known_titles = tfidfm
    cosine_similarities = cosine_similarity(new_i, known_titles)
    best = cosine_similarities.argsort()[0, -5:]
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
    opt = process.extractOne(some_string, some_list)
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
    codes = list(map(str, dataframe_row['top5']))
    index = codes.index
    options = {}
    items = options.items
    for c in codes:
        if dataframe_row['title_nospace'] != '':
            good_opt = extract(dataframe_row['title_nospace'], titles_mg[c])
            if good_opt[1] == 0:
                options[c] = ('None', '0')
            else:
                options[c] = (good_opt[0], good_opt[1], index(c))
            final_code = max(items(), key=lambda x: getKey(x[1]))
        else:
            final_code = (codes[4], ('None'))
    return final_code


def ascii_convert(cols, dfIn):
    """
    Function cleans ascii input and converts everything to strings in unicode
    """
    for col in cols:
        dfIn[col] = dfIn[col].astype(unicode)
        dfIn[col] = dfIn[col].apply(lambda x: x.encode('ascii', 'replace'))
        dfIn[col] = dfIn[col].astype(str)
    return dfIn
