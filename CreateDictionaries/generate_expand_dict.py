
# coding: utf-8
"""
Created on Tue Mar 28 09:38:19 2017

@author: 327660
"""

import pandas as pd
import pickle
import os
import json


directory = r"C:\Users\327660\Documents\Job_title_matching"
directory.replace('\\','/')
directory = os.path.normpath(directory)
os.chdir(directory)

#%% Create dictionary of abbreviations using manually curated csv
expand_abbreviations = pd.read_csv('replace_lookup_abbreviations.csv', encoding = 'utf-8')
expand_dict = {}
for ix, i in enumerate(expand_abbreviations.loc[:, 'Word']):
    expand_dict[i] = expand_abbreviations.loc[ix, 'Replace_with']
    
#%% Write dictionary to pickle and json
pickle.dump(expand_dict, open('expand_dict.pkl', 'wb'))

with open('expand_dict.json', 'w') as fp:
    json.dump(expand_dict, fp, indent = 4)
    