# -*- coding: utf-8 -*-
"""Main module."""

import os
import sys
import time

import numpy as np
import pandas as pd
# import modin.pandas as pd

from occupationcoder.coder import cleaner as cl
from occupationcoder.coder import code_matcher as cm

# Initialises Dask environment
# from distributed import Client
# client = Client()

#inFile = sys.argv[1]
#outFile = sys.argv[2]

## The options below can be used to run script with parameters from command line
#inFile = sys.argv[1]
#outFile = sys.argv[2]

script_dir = os.path.dirname(os.path.abspath('__file__'))
parent_dir = os.path.dirname(script_dir)
output_dir = os.path.join(parent_dir, 'outputs')


class Coder:
    """

    Codes job titles and descriptions into Standard Occupational
    Classification codes

    """
    # Columns needed for coding
    cols_to_process = ['job_title', 'job_description', 'job_sector']

    def __init__(self):
        self.data = []
        self.matcher = cm.MixedMatcher()

    def code_job_row(self, job_title, job_description, job_sector):
        df = pd.DataFrame([job_title, job_description, job_sector],
                          index=self.cols_to_process).T
        return self.code_data_frame(df)

    def code_data_frame(self, df_all):
        # Return an error if user has passed in columns which do not exist in the
        # data frame.
        # Remove any leading or trailing whitespace from column names

        # Test for missing data columns
        for col in self.cols_to_process:
            if col not in df_all.columns:
                sys.exit(("Occupationcoder message:\n") +
                         ("Please ensure a " + col +
                          " column exists in your csv file"))

        # Select and copy only data required for coding
        # TODO Fix inefficient memory use here
        df = df_all.copy()\
                   .rename(columns=dict(zip(df_all.columns,
                                            [x.lstrip().rstrip()
                                             for x in df_all.columns])))[self.cols_to_process]

        # Apply the cleaning function
        df['clean_title'] = df['job_title'].apply(cl.simple_clean)
        df['clean_sector'] = df['job_sector'].apply(lambda x: cl.simple_clean(x, known_only=False))
        df['clean_desc'] = df['job_description'].apply(lambda x: cl.simple_clean(x, known_only=False))

        # Combine cleaned job title, sector and description
        df['all_text'] = df[['clean_title', 'clean_sector', 'clean_desc']]\
            .apply(lambda x: ' '.join(x), axis=1)

        # Drop unused columns
        df.drop(['clean_sector', 'clean_desc'], inplace=True, axis=1)

        # Part II: Processing
        # Check for exact matches
        df['SOC_code'] = df['clean_title'].apply(lambda x: self.matcher.get_exact_match(x))

        # Apply fuzzy matching where no exact match is found
        df['SOC_code'] = np.where(df['SOC_code'].isna(),
                                  df['all_text'].apply(self.matcher.get_best_fuzzy_match),
                                  df['SOC_code'])

        # Return SOC_code to original dataframe, which contains all other columns
        df_all.loc[:, 'SOC_code'] = df.loc[:, 'SOC_code']
        return df_all


# Define main function. Main operations are placed here to make it possible
# use multiprocessing in Windows.
if __name__ == '__main__':
    # Read command line inputs
    inFile = sys.argv[1]
    df = pd.read_csv(inFile)
    commCoder = Coder()
    proc_tic = time.perf_counter()
    df = commCoder.code_data_frame(df)
    proc_toc = time.perf_counter()
    print("Actual coding ran in: {}".format(proc_toc - proc_tic))
    print("occupationcoder message:\n" +
          "Coding complete. Showing first results...")
    print(df.head())
    # Write to csv
    df.to_csv(os.path.join(script_dir,
                           'occupationcoder',
                           'outputs',
                           'processed_jobs.csv'),
              index=False,
              encoding='utf-8')
