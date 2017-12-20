# -*- coding: utf-8 -*-
"""
Created on Tue Feb 07 18:24:19 2017

@author: 327660
"""
import dask
import dask.dataframe as dd
#from dask.diagnostics import Profiler, ResourceProfiler, CacheProfiler
import pandas as pd
import os
import sys


#os.chdir('/Users/at/Documents/occupation-coder/Coder')
script_dir = os.path.dirname(os.path.abspath('__file__'))
parent_dir = os.path.join(script_dir, '..')
data_dir = os.path.join(parent_dir, 'TestVacancies')
output_dir = os.path.join(parent_dir, 'Outputs')
lookup_dir = os.path.join(parent_dir, 'Dictionaries')
utility_dir = os.path.join(parent_dir, 'Utilities')
sys.path.append(utility_dir)
import utilities as utils

inFile = sys.argv[1]
#outFile = sys.argv[2]

## The options below can be used to run script with parameters from command line
#inFile = sys.argv[1]
#outFile = sys.argv[2]


## Define main function. Main operations are placed here to make it possible
## use multiprocessing in Windows.
if __name__ == '__main__':
    ## Part I: Cleaning

    ## Read in dataframe

    df_all  = pd.read_csv(inFile)

    ## Only keep columns we need
    colsToProcess = ['job_title', 'job_description', 'job_sector']

    # Return an error if user has passed in columns which do not exist in the
    # data frame.
    for col in colsToProcess:
        if col not in df_all.columns:
            sys.exit( ("Occupation-coder message:\n")+
                ("Please ensure a "+col+" column exists in your csv file"))
    # Ensure it's all in unicode
    for col in colsToProcess:
        df_all[col] = df_all[col].apply(lambda x: unicode(str(x),'utf-8','ignore'))
    df = df_all[colsToProcess]

    ## Generate dask dataframe from pandas dataframe to enable multiprocessing
    ds = dd.from_pandas(df, npartitions = 2)

    ## Clean job title, job sector and description
    res1 = ds.apply(utils.clean_title, axis = 1,\
    meta = ('x', ds['job_title'].dtype))
    res2 = ds.apply(utils.clean_desc, axis = 1,\
    meta = ('x', ds['job_title'].dtype))
    res3 = ds.apply(utils.clean_sector, axis = 1,\
    meta = ('x', ds['job_title'].dtype))

    df['title_nospace'] =  res1.compute(get=dask.multiprocessing.get)
    df['desc_nospace'] = res2.compute(get=dask.multiprocessing.get)
    df['job_sector_nospace'] = res3.compute(get=dask.multiprocessing.get)

    ## Combine cleaned job title, sector and description
    df['title_and_desc'] = df[['title_nospace', 'job_sector_nospace','desc_nospace']]\
    .apply(lambda x: ' '.join(x), axis=1)

    ## Drop unused columns
    df.drop(['desc_nospace','job_sector_nospace'], inplace=True, axis=1)

    ## Part II: Processing
    ## Check for exact matches
    df['SOC_code'] = df['title_nospace'].apply(lambda x: utils.exact_match(x))
    matched = df[df['SOC_code'] !='NA']
    not_matched = df[df['SOC_code'] =='NA']

    ## Create dask dataframe from not_matched dataframe
    ds = dd.from_pandas(not_matched, npartitions = 4)

    ## Run function to obtain top 5 most similar minor groups
    ## Tfidf vectorisation is implemented inside the function utils.get_best_score_top5_2
    ds = ds.assign(top5 = ds['title_and_desc'].map(utils.get_best_score_top5_2))

    ## Run function to get best fuzzy match
    res = ds.apply(utils.return_best_match_2, axis = 1,
                   meta = ('x', ds['title_nospace'].dtype))
    x = res.compute(get=dask.multiprocessing.get)

    ## Write result back to pandas dataframe
    not_matched.loc[:,'SOC_code']=x.apply(lambda x: x[0])

    ## Merge matched and not_matched dataframes
    frames = [matched, not_matched]
    combined = pd.concat(frames)
    combined_sorted = combined.sort_index()

    ## Take SOC_code to original dataframe, which contains all other columns
    df_all.loc[:, 'SOC_code'] = combined_sorted.loc[:, 'SOC_code']

    ## Write to csv
    df_all.to_csv(os.path.join(output_dir, 'processed_jobs.csv'),
                  index = False,
                  encoding = 'utf-8')
    print(("Occupation-coder message:\n")+
        ("Coding complete"))
