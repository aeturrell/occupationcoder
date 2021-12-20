#!/usr/bin/env python

"""Tests for `occupationcoder` package."""


import unittest
import sys
import os
import subprocess

import pandas as pd

from occupationcoder.coder import coder
import occupationcoder.coder.cleaner as cl

EXPONENT = 4


class TestOccupationcoder(unittest.TestCase):
    """Tests for `occupationcoder` package."""

    def setUp(self):
        """Set up test fixtures, if any."""
        self.test_df = pd.read_csv(os.path.join("tests", "test_vacancies.csv"))
        # Multiply up that dataset to many, many rows so we can test time taken
        for i in range(EXPONENT):
            self.test_df = pd.concat([self.test_df.copy(), self.test_df.copy()], ignore_index=True)
        self.test_df = self.test_df.reset_index()
        print("Size of test dataset: {}".format(self.test_df.shape[0]))

    def tearDown(self):
        """Tear down test fixtures, if any."""

    def test_clean_titles(self):
        """ Checks that results of title cleaning are as expected """
        expected_answers = ['physicist', 'economist', 'ground worker']
        for index, row in self.test_df.iterrows():
            clean = cl.clean_title(row)
            self.assertIn(clean, expected_answers)

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
