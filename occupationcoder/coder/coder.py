# -*- coding: utf-8 -*-
"""Main module."""

import os
import json
import sys
import time

import numpy as np
import pandas as pd
# import modin.pandas as pd

# NLP related packages to support fuzzy-matching
from nltk import word_tokenize
from rapidfuzz import process, fuzz
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from occupationcoder.coder.cleaner import simple_clean

script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
lookup_dir = os.path.join(parent_dir, 'dictionaries')
output_dir = os.path.join(os.path.dirname(parent_dir), 'outputs')


class MixedMatcher:
    def __init__(self, lookup_dir=lookup_dir):

        # Load up the titles lists, ensure codes are loaded as strings...
        with open(os.path.join(lookup_dir, 'titles_minor_group_ons.json'), 'r') as infile:
            self.titles_mg = json.load(infile, parse_int=str)

        # Clean the job titles lists with the same code that will be used for the record titles/sectors/descriptions
        for SOC_code in self.titles_mg.keys():
            self.titles_mg[SOC_code] = [simple_clean(title, known_only=False)
                                        for title in self.titles_mg[SOC_code]]

        self.mg_buckets = pd.read_json(os.path.join(lookup_dir, 'mg_buckets_ons_df_processed.json'))\
                            .astype(str)

        # Build the TF-IDF model
        self._tfidf = TfidfVectorizer(tokenizer=word_tokenize,
                                      stop_words='english',
                                      ngram_range=(1, 3))

        # Store the matrix of SOC TF-IDF vectors
        self._SOC_tfidf_matrix = self._tfidf.fit_transform(self.mg_buckets.Titles_nospace)

    def get_exact_match(self, title: str):
        """ If exists, finds exact match to a job title's first three words """
        title = ' '.join(title.split()[:3])
        result = None
        keys = self.titles_mg.keys()

        # For each SOC code:
        for k in keys:
            # Check if exact job title is in its list of job titles
            if title in self.titles_mg[k]:
                result = k
        return result

    def get_tfidf_match(self, text, top_n=5):
        """ Finds the closest top_n matching SOC descriptions to some text """

        # Calculate similarities
        vector = self._tfidf.transform([text])
        sim_scores = cosine_similarity(vector, self._SOC_tfidf_matrix)

        # Return top_n highest scoring
        best = sim_scores.argsort()[0, -top_n:]
        return [self.mg_buckets.SOC_code[SOC_code] for SOC_code in best]

    def get_best_fuzzy_match(self, text, detailed_return=False):
        """
        Uses get_tfidf_match to narrow the options down to five possible SOC codes,
        then uses partial token set ratio in fuzzywuzzy to check against all individual
        job descriptions.
        """
        best_fit_codes = self.get_tfidf_match(text)

        options = []

        # Iterate through the best options TF-IDF similarity suggests
        for SOC_code in best_fit_codes:

            # Clean descriptions
            best_fuzzy_match = process.extractOne(text, self.titles_mg[SOC_code], scorer=fuzz.token_set_ratio)

            # Handle non-match by looking at match score
            if best_fuzzy_match[1] == 0:
                options.append((None, 0, None))
            else:
                # Record best match, the score, and the associated SOC code
                options.append((best_fuzzy_match[0],
                                best_fuzzy_match[1],
                                SOC_code))

        # The most probable industries are last - sort so that most probable are first,
        # In case of a draw, max will take first value only
        options.reverse()
        best = max(options, key=lambda x: x[1])

        # Return the best code, or code and diagnostics
        if detailed_return:
            return best
        return best[2]

    def code_record(self, title: str, sector: str = None, description: str = None):
        """ Codes an individual job record, with optional title and description """
        clean_title = simple_clean(title)

        # Try to code using exact title match (and save a lot of computation
        match = self.get_exact_match(clean_title)
        if match:
            return match

        # Gather all text data
        all_text = clean_title

        # Process sector
        if sector:
            clean_sector = simple_clean(sector, known_only=False)
            all_text = all_text + " " + clean_sector

        # Process description
        if description:
            clean_description = simple_clean(description, known_only=False)
            all_text = all_text + " " + clean_description

        # Find best fuzzy match possible with the data
        return self.get_best_fuzzy_match(all_text)

    def code_row(self, row):
        return self.code_record(row['job_title'],
                                row['job_sector'],
                                row['job_description'])

    def code_data_frame_simple(self, df_all):
        df_all['SOC_code'] = df_all.apply(self.code_row, axis=1)
        return df_all

    def code_data_frame(self, df_all: pd.DataFrame,
                        title_column: str = "job_title",
                        sector_column: str = "job_sector",
                        description_column: str = "job_description"):
        """
        Utility method for coding an entire pandas dataframe - gains some
        speed by using DF-specific methods to apply functions and filter data
        """
        # Test for missing data columns
        cols_to_process = [title_column, sector_column, description_column]
        for col in cols_to_process:
            if col not in df_all.columns:
                sys.exit("Occupationcoder message:\n" +
                         "Please ensure a " + col +
                         " column exists in your dataframe")

        # Select and copy only data required for coding
        df = df_all.copy()\
                   .rename(columns=dict(zip(df_all.columns,
                                            [x.lstrip().rstrip()
                                             for x in df_all.columns])))[cols_to_process]

        # Apply the cleaning function
        df['clean_title'] = df[title_column].apply(simple_clean)
        df['clean_sector'] = df[sector_column].apply(lambda x: simple_clean(x, known_only=False))
        df['clean_desc'] = df[description_column].apply(lambda x: simple_clean(x, known_only=False))

        # Combine cleaned job title, sector and description
        df['all_text'] = df[['clean_title', 'clean_sector', 'clean_desc']]\
            .apply(lambda x: ' '.join(x), axis=1)

        # Drop unused columns
        df.drop(['clean_sector', 'clean_desc'], inplace=True, axis=1)

        # Part II: Processing
        # Check for exact matches
        df['SOC_code'] = df['clean_title'].apply(lambda x: self.get_exact_match(x))

        # Apply fuzzy matching where no exact match is found
        df['SOC_code'] = np.where(df['SOC_code'].isna(),
                                  df['all_text'].apply(self.get_best_fuzzy_match),
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
    commCoder = MixedMatcher()
    proc_tic = time.perf_counter()
    df = commCoder.code_data_frame(df)
    proc_toc = time.perf_counter()
    print("Actual coding ran in: {}".format(proc_toc - proc_tic))
    print("occupationcoder message:\n" +
          "Coding complete. Showing first results...")
    print(df.head())
    # Write to csv
    df.to_csv(os.path.join(output_dir, 'processed_jobs.csv'),
              index=False,
              encoding='utf-8')
