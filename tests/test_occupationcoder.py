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
import occupationcoder.coder.code_matcher as cm

EXPONENT = 10


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
        self.matcher = cm.MixedMatcher()

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

    def test_000_code_single_row_in_script(self):
        """Running the base example in a script."""
        myCoder = coder.Coder()
        answer = myCoder.code_job_row('Physicist',
                                    'Calculations of the universe',
                                    'Professional scientific')
        self.assertEqual(answer['SOC_code'].iloc[0], '211')

    def test_001_running_on_a_file(self):
        """Running the included examples from a file."""
        myCoder = coder.Coder()
        df = pd.read_csv(os.path.join('tests', 'test_vacancies.csv'))
        df = myCoder.code_data_frame(df)
        self.assertEqual(df['SOC_code'].to_list(), ['211', '242', '912'])

    def test_002_command_line_use(self):
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
        Vanilla pandas:  3072 records in ~52 seconds on my laptop
        """
        # Multiply up that dataset to many, many rows so we can test time taken
        for i in range(EXPONENT):
            self.test_df = pd.concat([self.test_df.copy(), self.test_df.copy()], ignore_index=True)
        self.test_df = self.test_df.reset_index()
        print("Size of test dataset: {}".format(self.test_df.shape[0]))

        # Instantiate coder class
        myCoder = coder.Coder()

        # Time only the actual code assignment process
        proc_tic = time.perf_counter()
        df = myCoder.code_data_frame(self.test_df)
        proc_toc = time.perf_counter()
        print("Coding process ran in: {}".format(proc_toc - proc_tic))
