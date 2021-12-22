#!/usr/bin/env python

"""Tests for `occupationcoder` package."""

import unittest
import time
import sys
import os
import subprocess

import pandas as pd
# import modin.pandas as pd

from occupationcoder.coder import coder
import occupationcoder.coder.cleaner as cl

SAMPLE_SIZE = 15000


class TestOccupationcoder(unittest.TestCase):
    """Tests for `occupationcoder` package."""

    def setUp(self):
        """Set up test fixtures, if any."""
        # The expected cleaned titles to our test data
        self.expected_titles = ['physicist', 'economist', 'ground worker']
        # The SOC codes that TFIDF is expected to suggest for the three examples
        self.expected_codes = ['242', '311', '212', '215', '211',
                               '353', '412', '215', '242', '211',
                               '242', '533', '243', '912', '323']

        # Load the three example records
        self.test_df = pd.read_csv(os.path.join("tests", "test_vacancies.csv"))

        # Instantiate matching class
        self.matcher = coder.MixedMatcher()

    def tearDown(self):
        """Tear down test fixtures, if any."""

    def test_clean_titles(self):
        """ Checks that results of title cleaning are as expected """
        clean_titles = self.test_df['job_title'].apply(cl.simple_clean)
        for title in clean_titles:
            self.assertIn(title, self.expected_titles)

    def test_code_exact_matcher(self):
        """ Results of exact title matching """
        clean_titles = self.test_df['job_title'].apply(cl.simple_clean)
        matches = clean_titles.apply(self.matcher.get_exact_match)
        for match in matches:
            self.assertIn(match, ['211', '242', None])

    def test_code_tfidf_matcher(self):
        """ TF-IDF similarity suggestions for categories? """
        df = self.test_df.copy()
        df['clean_title'] = df['job_title'].apply(cl.simple_clean)
        df['clean_sector'] = df['job_sector'].apply(lambda x: cl.simple_clean(x, known_only=False))
        df['clean_desc'] = df['job_description'].apply(lambda x: cl.simple_clean(x, known_only=False))

        for index, row in df.iterrows():
            clean = " ".join([row['clean_title'], row['clean_sector'], row['clean_desc']])
            SOC_codes = self.matcher.get_tfidf_match(clean)
            for code in SOC_codes:
                self.assertIn(code, self.expected_codes)

    def test_code_fuzzy_matcher(self):
        """ For now, just tests that it runs - this is the expensive thing! """
        df = self.test_df.copy()
        df['clean_title'] = df['job_title'].apply(cl.simple_clean)
        df['clean_sector'] = df['job_sector'].apply(lambda x: cl.simple_clean(x, known_only=False))
        df['clean_desc'] = df['job_description'].apply(lambda x: cl.simple_clean(x, known_only=False))

        for index, row in df.iterrows():
            clean = " ".join([row['clean_title'], row['clean_sector'], row['clean_desc']])
            best_match = self.matcher.get_best_fuzzy_match(clean)

    def test_code_record(self):
        """ Confirm it correctly runs on our example single record """
        result = self.matcher.code_record(title='Physicist',
                                          sector='Professional scientific',
                                          description='Calculations of the universe')

        self.assertEqual(result, '211')

    def test_code_data_frame(self):
        """Running the included examples from a file."""
        df = pd.read_csv(os.path.join('tests', 'test_vacancies.csv'))
        df = self.matcher.code_data_frame(df,
                                          title_column="job_title",
                                          sector_column="job_sector",
                                          description_column="job_description")
        self.assertEqual(df['SOC_code'].to_list(), ['211', '242', '912'])

    def test_command_line(self):
        """ Test code execution at command line """

        # sys.executable returns current python executable, ensures code is run
        # in same environment from which tests are called
        subprocess.run([sys.executable, '-m',
                        'occupationcoder.coder.coder',
                        'tests/test_vacancies.csv'])
        df = pd.read_csv(os.path.join('occupationcoder',
                                      'outputs',
                                      'processed_jobs.csv'))
        self.assertEqual(df['SOC_code'].to_list(), [211, 242, 912])

    def manual_load_test(self):
        """
        Look at execution speed.
        Vanilla pandas:  15000 records in ~52 seconds on my laptop
        """
        # Multiply up that dataset to many, many rows so we can test time taken
        big_df = self.test_df.sample(SAMPLE_SIZE, replace=True, ignore_index=True)
        print("Size of test dataset: {}".format(big_df.shape[0]))

        # Time only the actual code assignment process
        proc_tic = time.perf_counter()
        _ = self.matcher.code_data_frame(big_df,
                                         title_column="job_title",
                                         sector_column="job_sector",
                                         description_column="job_description")
        print(_.shape)
        print(_[['job_title', 'SOC_code']].head(5))
        proc_toc = time.perf_counter()
        print("Coding process ran in: {}".format(proc_toc - proc_tic))
