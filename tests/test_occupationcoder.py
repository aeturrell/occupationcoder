#!/usr/bin/env python

"""Tests for `occupationcoder` package."""


import unittest
from occupationcoder.coder import coder
import pandas as pd
import os
import subprocess


class TestOccupationcoder(unittest.TestCase):
    """Tests for `occupationcoder` package."""

    def setUp(self):
        """Set up test fixtures, if any."""

    def tearDown(self):
        """Tear down test fixtures, if any."""

    def test_000_code_single_row_in_script(self):
        """Running the base example in a script."""
        myCoder = coder.Coder()
        answer = myCoder.codejobrow('Physicist', 'Calculations of the universe', 'Professional scientific')
        self.assertEqual(answer['SOC_code'].iloc[0], '211')
    
    def test_001_running_on_a_file(self):
        """Running the included examples from a file."""
        myCoder = coder.Coder()
        df = pd.read_csv(os.path.join('tests', 'test_vacancies.csv'))
        df = myCoder.codedataframe(df)
        self.assertEqual(df['SOC_code'].to_list(), ['211', '242'])
    
    def test_002_command_line_use(self):
        subprocess.run(['python', '-m',
                        'occupationcoder.coder.coder',
                        'tests/test_vacancies.csv'])
        df = pd.read_csv(os.path.join('occupationcoder',
                                      'outputs',
                                      'processed_jobs.csv'))
        self.assertEqual(df['SOC_code'].to_list(), [211, 242])
