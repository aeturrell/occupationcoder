#!/usr/bin/env python

"""Tests for `occupationcoder` package."""

import unittest
import sys
import os
import subprocess

import pandas as pd

from occupationcoder.coder import coder
import occupationcoder.coder.cleaner as cl
import occupationcoder.coder.code_matcher as cm

EXPONENT = 1


class TestOccupationcoder(unittest.TestCase):
    """Tests for `occupationcoder` package."""

    def setUp(self):
        """Set up test fixtures, if any."""
        # The expected cleaned titles to our test data
        self.expected_titles = ['physicist', 'economist', 'ground worker']
        # The SOC codes that TFIDF is expected to suggest for the three examples
        self.expected_codes = [242, 311, 212, 215, 211,
                               353, 412, 215, 242, 211,
                               242, 533, 243, 912, 323]

        self.test_df = pd.read_csv(os.path.join("tests", "test_vacancies.csv"))

        # Multiply up that dataset to many, many rows so we can test time taken
        for i in range(EXPONENT):
            self.test_df = pd.concat([self.test_df.copy(), self.test_df.copy()], ignore_index=True)
        self.test_df = self.test_df.reset_index()
        print("Size of test dataset: {}".format(self.test_df.shape[0]))

        # Instantiate matching class
        self.matcher = cm.MixedMatcher()

    def tearDown(self):
        """Tear down test fixtures, if any."""

    def test_clean_titles(self):
        """ Checks that results of title cleaning are as expected """
        for index, row in self.test_df.iterrows():
            clean = cl.clean_title(row)
            self.assertIn(clean, self.expected_titles)

    def test_code_exact_matcher(self):
        """ Results of exact title matching """
        for index, row in self.test_df.iterrows():
            # Create whole-text string
            clean = cl.clean_title(row)
            match = self.matcher.get_exact_match(clean)
            self.assertIn(match, ['211', '242', None])

    def test_code_tfidf_matcher(self):
        """ TF-IDF similarity suggestions for categories? """
        for index, row in self.test_df.iterrows():
            clean = " ".join([cl.clean_title(row), cl.clean_sector(row), cl.clean_desc(row)])
            SOC_codes = self.matcher.get_tfidf_match(clean)
            for code in SOC_codes:
                self.assertIn(code, self.expected_codes)

    def test_code_fuzzy_matcher(self):
        """ For now, just tests that it runs """
        for index, row in self.test_df.iterrows():
            clean = " ".join([cl.clean_title(row), cl.clean_sector(row), cl.clean_desc(row)])
            best_match = self.matcher.get_best_fuzzy_match(clean)
            print(row['job_title'])
            for index, value in best_match.items():
                print(index, value[1], value[0])
            print("\n")

    def test_000_code_single_row_in_script(self):
        """Running the base example in a script."""
        myCoder = coder.Coder()
        answer = myCoder.codejobrow('Physicist',
                                    'Calculations of the universe',
                                    'Professional scientific')
        self.assertEqual(answer['SOC_code'].iloc[0], '211')

    def test_001_running_on_a_file(self):
        """Running the included examples from a file."""
        myCoder = coder.Coder()
        df = pd.read_csv(os.path.join('tests', 'test_vacancies.csv'))
        df = myCoder.codedataframe(df)
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
