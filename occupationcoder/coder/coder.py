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

        # Placeholder, column names for fields needed for coding
        self.df_columns = {"title": None,
                           "sector": None,
                           "description": None}

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
        """
        Codes an individual job title, with optional sector and description text

        Keyword arguments:
            title -- freetext job title to find a SOC code for
            sector -- any additional description of industry/sector
            description -- freetext description of work/role/duties
        """
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

    def _code_row(self, row):
        """ Helper for applying code_record over the rows of a pandas DataFrame"""
        return self.code_record(row[self.df_columns['title']],
                                row[self.df_columns['sector']],
                                row[self.df_columns['description']])

    def code_data_frame(self, record_df,
                        title_column: str = 'job_title',
                        sector_column: str = None,
                        description_column: str = None):
        """
        Applies tool to all rows in a provided pandas DataFrame

        Keyword arguments:
            record_df -- Pandas dataframe containing columns named:
            title_column -- Freetext job title to find a SOC code for (default 'job_title')
            sector_column -- Any additional description of industry/sector (default None)
            description_column -- Freetext description of work/role/duties (default None)
        """
        # Record the column names for later
        self.df_columns.update({"title": title_column,
                                "sector": sector_column,
                                "description": description_column})

        record_df['SOC_code'] = record_df.apply(self._code_row, axis=1)
        return record_df


# Define main function. Main operations are placed here to make it possible
# use multiprocessing in Windows.
if __name__ == '__main__':
    # Read command line inputs
    inFile = sys.argv[1]
    df = pd.read_csv(inFile)
    commCoder = MixedMatcher()
    proc_tic = time.perf_counter()
    df = commCoder.code_data_frame(df,
                                   title_column="job_title",
                                   sector_column="job_sector",
                                   description_column="job_description")
    proc_toc = time.perf_counter()
    print("Actual coding ran in: {}".format(proc_toc - proc_tic))
    print("occupationcoder message:\n" +
          "Coding complete. Showing first results...")
    print(df.head())
    # Write to csv
    df.to_csv(os.path.join(output_dir, 'processed_jobs.csv'),
              index=False,
              encoding='utf-8')
