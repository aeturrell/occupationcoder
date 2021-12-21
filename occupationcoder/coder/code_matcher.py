import os
import json

import pandas as pd

from nltk import word_tokenize
from fuzzywuzzy import process, fuzz
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

import occupationcoder.coder.cleaner as cl

script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
lookup_dir = os.path.join(parent_dir, 'dictionaries')


class MixedMatcher:
    def __init__(self, lookup_dir=lookup_dir):

        # Load up the titles lists, ensure codes are loaded as strings...
        with open(os.path.join(lookup_dir, 'titles_minor_group_ons.json'), 'r') as infile:
            self.titles_mg = json.load(infile, parse_int=str)

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

    def get_best_fuzzy_match(self, text):

        best_fit_codes = self.get_tfidf_match(text)

        options = {}

        # Iterate through the best options TF-IDF similarity suggests
        for SOC_code in best_fit_codes:

            if not SOC_code:
                return (best_fit_codes[4], (None))

            # Clean descriptions
            clean_SOC_descriptions = [cl.simple_clean(description) for description in self.titles_mg[SOC_code]]
            best_fuzzy_match = process.extractOne(text, clean_SOC_descriptions, scorer=fuzz.token_set_ratio)

            # Handle non-match by looking at match score
            if best_fuzzy_match[1] == 0:
                options[SOC_code] = (None, '0')
            else:
                # Record best match, the score, and the associated SOC code
                options[SOC_code] = (best_fuzzy_match[0],
                                     best_fuzzy_match[1],
                                     text[:50])

        # TODO: IMPLEMENT SELECTION OF BEST
        return options
        # return max(options.items(), key=lambda x: x[1])
