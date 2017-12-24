
# coding: utf-8
"""
Created on Tue Mar 28 09:38:19 2017

@author: 327660
"""

import pandas as pd
import pickle
import os
import json

script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.join(script_dir, '..')
lookup_dir = os.path.join(parent_dir, 'Dictionaries')

# Create dictionary of abbreviations using manually curated csv
expand_abbreviations = pd.read_csv(os.path.join(lookup_dir, 
                                   'replace_lookup_abbreviations.csv'), 
                                    encoding = 'utf-8')
expand_dict = {}
for ix, i in enumerate(expand_abbreviations.loc[:, 'Word']):
    expand_dict[i] = expand_abbreviations.loc[ix, 'Replace_with']
    
# Write dictionary to pickle and json
#pickle.dump(expand_dict, open(os.path.join(lookup_dir, 'expand_dict.pkl'), 'wb'))

with open(os.path.join(lookup_dir, 'expand_dict.json'), 'w') as fp:
    json.dump(expand_dict, fp, indent = 4)
    