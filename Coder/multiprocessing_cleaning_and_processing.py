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
import utilities as utils

## The options below can be used to run script with parameters from command line

#inFile = sys.argv[1]
#outFile = sys.argv[2]



## Define functions




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
