# -*- coding: utf-8 -*-
"""Main module."""

import dask
import dask.dataframe as dd
import pandas as pd
import os
import sys
from occupationcoder.utilities import utilities as utils
import json


#inFile = sys.argv[1]
#outFile = sys.argv[2]

## The options below can be used to run script with parameters from command line
#inFile = sys.argv[1]
#outFile = sys.argv[2]

script_dir = os.path.dirname(os.path.abspath('__file__'))
parent_dir = os.path.dirname(script_dir)
output_dir = os.path.join(parent_dir,'outputs')
util_dir = os.path.join(parent_dir, 'utilities')
sys.path.append(util_dir)
# from .utilities import utilities as utils

class Coder:
    """

    Codes job titles and descriptions into Standard Occupational
    Classification codes

    """
    ## Only keep columns we need
    colsToProcess = ['job_title', 'job_description', 'job_sector']
    def __init__(self):
        self.data = []

    def codejobrow(self,job_title,job_description,job_sector):
        df = pd.DataFrame([job_title,job_description,job_sector],
        index=self.colsToProcess).T
        return self.codedataframe(df)

    def codedataframe(self,df_all):
        # Return an error if user has passed in columns which do not exist in the
        # data frame.
        # Remove any leading or trailing whitespace from column names
        df_all = df_all.rename(columns=dict(zip(df_all.columns,
                                [x.lstrip().rstrip() for x in df_all.columns])))
        for col in self.colsToProcess:
            if col not in df_all.columns:
                sys.exit( ("Occupationcoder message:\n")+
                    ("Please ensure a "+col+" column exists in your csv file"))
        # Ensure it's all in unicode
        # for col in self.colsToProcess:
        #     df_all[col] = df_all[col].apply(lambda x: str(x).encode('utf-8', 'ignore'))
        df = df_all[self.colsToProcess]

        ## Generate dask dataframe from pandas dataframe to enable multiprocessing
        ds = dd.from_pandas(df, npartitions = 2)

        ## Clean job title, job sector and description
        datatype = ds[self.colsToProcess[0]].dtype
        res1 = ds.apply(utils.clean_title, axis = 1,\
        meta = ('x', datatype))
        res2 = ds.apply(utils.clean_desc, axis = 1,\
        meta = ('x', datatype))
        res3 = ds.apply(utils.clean_sector, axis = 1,\
        meta = ('x', datatype))

        df['title_nospace'] = res1.compute(scheduler='processes')
        df['desc_nospace'] = res2.compute(scheduler='processes')
        df['job_sector_nospace'] = res3.compute(scheduler='processes')

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
        x = res.compute(scheduler='processes')

        ## Write result back to pandas dataframe
        not_matched.loc[:,'SOC_code']=x.apply(lambda x: x[0])

        ## Merge matched and not_matched dataframes
        frames = [matched, not_matched]
        combined = pd.concat(frames)
        combined_sorted = combined.sort_index()

        ## Take SOC_code to original dataframe, which contains all other columns
        df_all.loc[:, 'SOC_code'] = combined_sorted.loc[:, 'SOC_code']
        return df_all


## Define main function. Main operations are placed here to make it possible
## use multiprocessing in Windows.
if __name__ == '__main__':
    # Read command line inputs
    inFile = sys.argv[1]
    df = pd.read_csv(inFile)
    commCoder = Coder()
    df = commCoder.codedataframe(df)
    print(("occupationcoder message:\n")+
        ("Coding complete. Showing first results..."))
    print(df.head())
    ## Write to csv
    df.to_csv(os.path.join(script_dir,
    'occupationcoder',
    'outputs',
    'processed_jobs.csv'),
                  index = False,
                  encoding = 'utf-8')