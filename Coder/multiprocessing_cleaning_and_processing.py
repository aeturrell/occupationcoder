# -*- coding: utf-8 -*-
"""
Created on Tue Feb 07 18:24:19 2017

@author: 327660
"""
import dask
import dask.dataframe as dd
from dask.diagnostics import Profiler, ResourceProfiler, CacheProfiler
import pandas as pd
import os
import nltk
import pickle
import re
import sys
import json


## The options below can be used to run script with parameters from command line

#inFile = sys.argv[1]
#outFile = sys.argv[2]



## Define functions

def lemmatise(title_terms):
    '''
    Takes list as input.
    Removes suffixes if the new words exists in the nltk dictionary.
    The purpose of the function is to convert plural forms into singular.
    Allows some nouns to remain in plural form (the to_keep_asis is manually curated).
    Returns a list.

    In [16]: lemmatise(['teachers'])
    Out[16]: [u'teacher']

    In [17]: lemmatise(['analytics'])
    Out[17]: ['analytics']
    '''
    to_keep_asis = ['sales', 'years', 'goods', 'operations', 'systems',
                    'communications', 'events', 'loans', 'grounds',
                    'lettings', 'claims', 'accounts', 'relations',
                    'complaints', 'services']
    wnl = nltk.WordNetLemmatizer()
    processed_terms = [wnl.lemmatize(i) if i not in to_keep_asis else i for i in title_terms]
    return processed_terms


def replace_word(word, lookup_dict):
    '''
    Takes string and a look up dictionary as input.
    The input string is used as a key in a dictionary. Function returns a value
    in a string format for the specified key in a provided dictionary.

    In [8]: replace_word('rgn', expand_dict)
    Out[8]: u'registered general nurse'
    '''
    word=lookup_dict[word]
    return word


def lookup_replacement(words, lookup_dict):
    '''
    Takes a list and a dict as input, replaces words in the list if they exist
    in the dict as keys with corresponding values .
    Returns a list.
    This function is used to expand abbreviations.

    In [10]: lookup_replacement(['pa', 'to', 'vice', 'president'], expand_dict)
    Out[10]: [u'personal assistant', 'to', 'vice', 'president']
    '''
    keys = expand_dict.keys()
    this_dict = expand_dict
    words = [replace_word(word, this_dict) if word in keys else word for word in words]
    return words


def replace_unknown(s):
    '''
    Takes a string and a ONS known words dict as input.
    Only keeps the words that exist in ONS title words.
    Returns a string.

    In [14]: replace_unknown('high court enforcement agent london x2')
    Out[14]: 'high court enforcement agent'
    '''
    all_words = s.split()
    tokeep = []
    keys = known_words_dict.keys()
    tokeep = [i for i in all_words if i in keys]
    clean_words = ' '.join(tokeep)
    return clean_words


def remove_digits(s):
    '''
    Takes a string as input.
    Removes digits in a string.
    Returns a string.

    In [16]: remove_digits('2 recruitment consultants')
    Out[16]: ' recruitment consultants'
    '''
    result = ''.join(i for i in s if not i.isdigit())
    return result


select_punct = set('!"#$%&\()*+,-./:;<=>?@[\\]^_`{|}~') #only removed "'"
def replace_punctuation(s):
    '''
    Takes string as input.
    Removes punctuation from a string if the character is in select_punct.
    Returns a string.

    In [18]: replace_punctuation('sales executives/ - london')
    Out[18]: 'sales executives   london'
    '''
    for i in set(select_punct):
        if i in s:
            s = s.replace(i, ' ')
    return s


from HTMLParser import HTMLParser
class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ' '.join(self.fed)


def strip_tags(html):
    '''
    Takes string as input.
    Removes html tags.
    Returns a string.

    '''
    s = MLStripper()
    s.feed(html)
    return s.get_data()


def clean_title(x):
    '''
    Takes string in a dataframe column 'job_title' as input. In sequence applies functions to lemmatise terms,
    expand abbreviations, replace words not in ONS classification, remove digits,
    remove punctuaiton and extra spaces. This function is adapted to work with
    dataframe fields and not individual strings.
    Returns a string.

    In [36]: sample_df['job_title']
    Out[36]:
    0             recruitment consultant  be your own boss
    1    financial consultant£4284633462 plus bonus an...
    2    dutch speaking contact services representative...
    3                               care assistant  worker
    4                               care assistant  worker
    Name: job_title, dtype: object

    In [37]: sample_df.apply(lambda x: clean_title(x), axis=1)
    Out[37]:
    0             recruitment consultant
    1       financial plus bonus and car
    2    contact services representative
    3              care assistant worker
    4              care assistant worker
    dtype: object
    '''
    lemm = lemmatise(x['job_title'].split())
    exp = lookup_replacement(lemm, expand_dict)
    known = replace_unknown(' '.join(exp)).strip()
    nodigits = remove_digits(known)
    nopunct = replace_punctuation(nodigits)
    nospace = re.sub(' +',' ',nopunct)
    nospace = nospace.strip()
    return nospace

def clean_desc(x):
    '''
    Takes string in a dataframe field 'job_description' as input. In sequence
    applies functions to lemmatise terms, expand abbreviations, remove punctuaiton
    and extra spaces. This function is adapted to work with    dataframe fields
    and not individual strings.
    Returns a string.

    '''
    lower = strip_tags(x['job_description']).lower()
    lemm = lemmatise(lower.split())
    exp = lookup_replacement(lemm, expand_dict)
    nopunct = replace_punctuation(' '.join(exp))
    nospace = re.sub(' +',' ',nopunct)
    nospace = nospace.strip()
    return nospace

def clean_sector(x):
    '''
    Takes string in a dataframe field 'job_sector' as input. In sequence
    applies functions to collapse case, replace 'other' with ' ', expand
    abbreviations, remove punctuaiton and extra spaces.
    This function is adapted to work with dataframe fields and not individual
    strings.
    Returns a string.

    In [50]: sample_df['job_sector']
    Out[50]:

    93                Social Care
    94     Hospitality & Catering
    95    Recruitment Consultancy
    96                Social Care
    97             Marketing & PR
    98                   Training
    Name: job_sector, dtype: object

    In [51]: sample_df.apply(lambda x: clean_sector(x), axis=1)
    Out[51]:

    93                social care
    94       hospitality catering
    95    recruitment consultancy
    96                social care
    97               marketing pr
    98                   training
    dtype: object
    '''
    lower = replace_punctuation(x['job_sector'].lower())
    replaced = lower.replace('other', ' ')
    exp = lookup_replacement(replaced.split(), expand_dict)
    nospace = re.sub(' +',' ',' '.join(exp))
    nospace = nospace.strip()
    return nospace


def exact_match(x):
    '''
    Takes string as input.
    Checks if the first three words in a job title match exactly any known
    titles in the titles_mg dict.
    Returns matching SOC code if there is an exact match or 'NA' if not.

    In [54]: exact_match('medical writer')
    Out[54]: '341'

    In [55]: exact_match('professional medical writer')
    Out[55]: 'NA'
    '''
    title = ' '.join(x.split()[:3])
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
    '''
    Takes string as input.
    Returns list of tokens. The function is used as an argument for
    TfidfVectorizer.

    In [57]: tokenize('some job title')
    Out[57]: ['some', 'job', 'title']
    '''
    tokens = nltk.word_tokenize(text)
    return tokens


def get_best_score_top5_2(x):
    '''
    Takes string as input.
    Generates tf-idf for a raw title, then calculates cosine similarity to each
    of 90 ONS minor group title and description buckets and picks 5 buckets
    with the highest cosine similarity.
    Returns a list of SOC_codes for the minor groups (in ascending order by
    cosine similarity).

    In [60]: from sklearn.feature_extraction.text import TfidfVectorizer
        ...: from sklearn.metrics.pairwise import cosine_similarity

    In [61]: mg_buckets = pd.read_pickle(directory+'\\lookups\\mg_buckets_ons_df_processed.pkl')

    In [62]: textfortoken= mg_buckets.Titles_nospace
        ...: tfidf = TfidfVectorizer(tokenizer=tokenize, stop_words='english',ngram_range=(1,3))
        ...: tfidfm = tfidf.fit_transform(textfortoken)

    In [63]: get_best_score_top5_2('community nurse')
    Out[63]: ['331', '612', '323', '614', '223']
    '''
    new_i = tfidf.transform([x])
    known_titles = tfidfm
    cosine_similarities = cosine_similarity(new_i, known_titles)
    best = cosine_similarities.argsort()[0,-5:]
    result = []
    append = result.append
    for b in best:
        append(mg_buckets.SOC_code[b])
    return result


def getKey(item):
    '''
    Takes any iterable as input.
    Returns tuple.
    Is used to specify order of argument importance, which can be used for
    sorting and gettin max using multiple criteria.

    In [66]: getKey(('registered nurse', 90, 4))
    Out[66]: (90, 4, 2)
    '''
    return (item[1], item[2], len(item[0].split()))


def getKey2(item):
    '''
    Takes any iterable as input.
    Returns tuple.
    Is used to specify order of importance, which can be used for sort and max.
    '''
    return (item[1][0], item[2])


def extract(x, some_list):
    '''
    Takes a string and a list as input. Uses fuzzywuzzy process.extractOne
    method to extract the item with the highest fuzzy match to input string
    from the provided list.

    In [69]: extract('recruitment consultant', ['recruitment coordinator',
    'director of recruitment', 'management consultant'])
    Out[69]: ('recruitment coordinator', 76)
    '''
    from fuzzywuzzy import process
    opt = process.extractOne(str(x), some_list)
    return opt


def return_best_match_2(x):
    '''
    Takes dataframe row as input.
    Finds the match for 'title_nospace' field with the highest fuzzy score out
    of all known titles in the top 5 ONS minor group buckets stored in 'top5'.
    This function is adapted to work with dataframe fields and not individual
    strings.
    Returns a tuple.

    In [71]: sample_df.loc[95:, ['title_nospace', 'job_sector_nospace']]
    Out[71]:
                                   title_nospace       job_sector_nospace
    95                    recruitment consultant  recruitment consultancy
    96                     care assistant worker              social care
    97  field marketing and business development             marketing pr
    98                          personal trainer                 training
    99                     care assistant worker              social care

    In [72]: sample_df['title_nospace'] = sample_df.apply(lambda x:\
    clean_title(x), axis=1)

    In [73]: sample_df['desc_nospace'] = sample_df.apply(lambda x:\
    clean_desc(x), axis=1)

    In [74]: sample_df['job_sector_nospace'] = sample_df.apply(lambda x:\
    clean_sector(x), axis=1)

    In [75]: sample_df['title_and_desc'] = sample_df[['title_nospace',\
    'job_sector_nospace','desc_nospace']].apply(lambda x: ' '.join(x), axis=1)

    In [76]: sample_df.loc[:,'top5'] = sample_df['title_and_desc'].\
    map(get_best_score_top5_2)

    In [77]: res = sample_df.apply(return_best_match_2, axis=1)

    In [78]: res.tail()
    Out[78]:
    95      (356, (recruitment consultant, 100, 1))
    96               (614, (care assistant, 90, 4))
    97    (354, (business sales executives, 86, 4))
    98            (344, (personal trainer, 100, 4))
    99               (614, (care assistant, 90, 4))
    dtype: object
    '''
    codes = x['top5']
    index = codes.index
    options ={}
    items = options.items
    for c in codes:
        if x['title_nospace'] != '':
            good_opt = extract(str(x['title_nospace']), titles_mg[c])
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


## Define main function. Main operations are placed here to make it possible
## use multiprocessing.

if __name__ == '__main__':

    ## Part I: Cleaning

    ## Read in dataframe
    root_directory = os.path.realpath("TestVacancies")
    #test_directory = r"/Users/at/Documents/occupation-coder/TestVacancies"
    df_all  = pd.read_csv(os.path.join(root_directory,'test_vacancies.csv'),
                      nrows = 1000)
    # Return an error if user has passed in columns which do not exist in the
    # data frame.

    ## Only keep columns we need
    colsToProcess = ['job_title', 'job_description', 'job_sector']
    df = df_all[colsToProcess]
    del(df_all)
    ## Handle ascii converter errors
    #df = ascii_convert(colsToProcess,df)
    ## Read in lookups
    dictionary_path = os.path.realpath(os.path.join(root_directory,
                                                    "Dictionaries"))
    known_words_dict=pd.read_json(os.path.join(dictionary_path,
                                                   "known_words_dict.json"))

    with open(dictionary_path+'\\lookups\\known_words_dict.pkl', 'r') as infile:
        known_words_dict = pickle.load(infile)

    with open(dictionary_path+'\\lookups\\expand_dict.pkl', 'r') as infile:
        expand_dict = pickle.load(infile)

    ## Generate dask dataframe from pandas dataframe to enable multiprocessing

    prof1=Profiler()
    prof1.register()

    rprof1 = ResourceProfiler()
    rprof1.register()

    ds = dd.from_pandas(df, npartitions = 4)

    ## Clean job title, job sector and description

    res1 = ds.apply(lambda x: clean_title(x), axis = 1,\
    meta = ('x', ds['job_title'].dtype))
    res2 = ds.apply(lambda x: clean_desc(x), axis = 1,\
    meta = ('x', ds['job_title'].dtype))
    res3 = ds.apply(lambda x: clean_sector(x), axis = 1,\
    meta = ('x', ds['job_title'].dtype))



    x = res1.compute(get=dask.multiprocessing.get)
    y = res2.compute(get=dask.multiprocessing.get)
    z = res3.compute(get=dask.multiprocessing.get)


    df['title_nospace'] =  x
    df['desc_nospace'] = y
    df['job_sector_nospace'] = z

    ## Combine cleaned job title, sector and description

    df['title_and_desc'] = df[['title_nospace', 'job_sector_nospace','desc_nospace']]\
    .apply(lambda x: ' '.join(x), axis=1)

    ## Drop unused columns

    df.drop(['desc_nospace','job_sector_nospace'], inplace=True, axis=1)

    ## Part II: Processing

    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity

    ## Read in lookups

    with open(directory+'\\lookups\\titles_minor_group_ons.pkl', 'r') as infile:
        titles_mg = pickle.load(infile)

    mg_buckets = pd.read_pickle(directory+'\\lookups\\mg_buckets_ons_df_processed.pkl')

    ## Check for exact matches

    df['SOC_code'] = df['title_nospace'].apply(lambda x: exact_match(x))
    matched = df[df['SOC_code'] !='NA']
    not_matched = df[df['SOC_code'] =='NA']

    ## Create dask dataframe from not_matched dataframe

    ds = dd.from_pandas(not_matched, npartitions = 4)

    ## Generate tf-idf weights using ONS reference

    textfortoken= mg_buckets.Titles_nospace
    tfidf = TfidfVectorizer(tokenizer=tokenize, stop_words='english',ngram_range=(1,3))
    tfidfm = tfidf.fit_transform(textfortoken)

    ## Run function to obtain top 5 most similar minor groups


    prof2=Profiler()
    prof2.register()
    ds = ds.assign(top5 = ds['title_and_desc'].map(get_best_score_top5_2))

    ## Run function to get best fuzzy match

    res = ds.apply(return_best_match_2, axis = 1, meta = ('x', ds['title_nospace'].dtype))
    x = res.compute(get=dask.multiprocessing.get)

    ## Write result back to pandas dataframe

    not_matched.loc[:,'SOC_code']=x.apply(lambda x: x[0])

    ## Merge matched and not_matched dataframes

    frames = [matched, not_matched]
    combined = pd.concat(frames)
    combined_sorted = combined.sort_index()

    ## Take SOC_code to original dataframe, which contains all other columns

#    df_all.loc[:, 'SOC_code'] = combined_sorted.loc[:, 'SOC_code']

    ## Write to pickle

#    df_all.to_pickle(directory+'\output\Weights_Regions_Vacancies_2016_withSOC.pkl')
