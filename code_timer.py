import time
import os
import sys
import subprocess

import pandas as pd

from occupationcoder.coder import coder

myCoder = coder.Coder()

# Only want to time application of the coder
tic = time.perf_counter()

subprocess.run([sys.executable, '-m',
                        'occupationcoder.coder.coder',
                        'tests/test_vacancies.csv'])
df = pd.read_csv(os.path.join('occupationcoder',
                                      'outputs',
                                      'processed_jobs.csv'))
toc = time.perf_counter()

print("Whole program ran in: {}".format(toc - tic))
