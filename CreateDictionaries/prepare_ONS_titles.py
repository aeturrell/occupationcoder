
# coding: utf-8
"""
Created on Tue Mar 28 09:38:19 2017

@author: 327660
"""

import pandas as pd
import pickle
import re
import os
import json
import sys

script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.join(script_dir, '..')
lookup_dir = os.path.join(parent_dir, 'Dictionaries')
utility_dir = os.path.join(parent_dir, 'Utilities')

sys.path.append(utility_dir)
import utilities as utils

# Read in Excel file with ONS Index
ONSdf =pd.read_excel(os.path.join(lookup_dir, 'ONS_index.xls'), 
                     sheetname = 'SOC2010 Full Index V5', 
                     encoding = 'utf-8')

#Only keep columns of interest
ONSdf = ONSdf[['SOC 2010', 'INDEXOCC', 'IND', 'ADD', 'SEE']]

# Process alternative titles from ONS Index

# Reverse order of words in titles
ONSdf['reversed_title'] = ONSdf['INDEXOCC'].apply(lambda x: x.split(', ')[::-1])

# Join back into single string
ONSdf['reversed_title2'] = ONSdf['reversed_title'].apply(lambda x: ' '.join(x))

# Replace NaN values with ''
ONSdf['clean_add'] = ONSdf['ADD'].fillna('')
ONSdf['clean_ind'] = ONSdf['IND'].fillna('')

# Add industrial and additional info to the title
ONSdf['complete_title'] = ONSdf[['reversed_title2', 'clean_add', 'clean_ind']].\
apply(lambda x: ' '.join(x), axis=1)

# Remove punctuation and strip out white spaces
ONSdf['complete_title2'] = ONSdf['complete_title'].\
apply(lambda x: utils.replace_punctuation(x).strip().lower())

# Remove extra white spaces
ONSdf['complete_title3'] = ONSdf['complete_title2'].\
apply(lambda x: re.sub(' +',' ',x))

reducedONSdf = ONSdf[['SOC 2010', 'complete_title3']]

# Generate 3 digit Minor group SOC code
reducedONSdf.loc[:,'SOC_Minor_group'] = reducedONSdf.loc[:, 'SOC 2010'].\
apply(lambda x: str(x)[:3])

#reducedONSdf.to_pickle('reducedONSdf.pkl')

# Process official unit group and minor group titles from ONS classification

# Read in minor group structure
minor_group = pd.read_excel(os.path.join(lookup_dir, 'ons_soc_structure_minor_group.xlsx'), 
                            encoding = 'utf-8')
minor_group.columns = ['Minor', 'Title']
minor_group.loc[:, 'Minor'] = minor_group.loc[:, 'Minor'].astype(str)

# Read in unit group structure
unit_group = pd.read_excel(os.path.join(lookup_dir, 'ons_soc_structure.xlsx'), 
                           sheetname = 'desc', 
                           encoding = 'utf-8')
unit_group.columns = ['Unit', 'Title', 'Description']

# Generate 3 digit Minor group SOC code
unit_group.loc[:,'Minor'] = unit_group.loc[:, 'Unit'].apply(lambda x: str(x)[:3])

# Collapse case and strip whitespaces for minor groups
minor_group.loc[:, 'Title'] = minor_group.loc[:, 'Title'].\
apply(lambda x: x.lower().strip())

# Collapse case and strip whitespaces for unit groups
unit_group.loc[:, 'Title'] = unit_group.loc[:, 'Title'].\
apply(lambda x: x.lower().strip())
unit_group.loc[:, 'Description'] = unit_group.loc[:, 'Description'].\
apply(lambda x: x.lower())

# Create title dictionary

# Start with minor group titles: key (minor group SOC), value (list of titles)
titles_mg = {}
for ix, t in enumerate(minor_group.loc[:, 'Title']):
    titles_mg[minor_group.loc[ix, 'Minor']]=[t]

# Update dict with unit group titles
for ix, t in enumerate(unit_group.loc[:, 'Minor']):
    if t in titles_mg.keys():
        titles_mg[t].append(unit_group.loc[ix, 'Title'])

# Add alternative titles from ONS index
for ix, t in enumerate(reducedONSdf.loc[:, 'SOC_Minor_group']):
    if t in titles_mg.keys():
        titles_mg[t].append(reducedONSdf.loc[ix, 'complete_title3'])
        
# Write dictionary without descriptions to pickle and json
#pickle.dump(titles_mg, open(os.path.join(lookup_dir, 'titles_mg_ons_nodesc.pkl'), 'wb'))

with open(os.path.join(lookup_dir, 'titles_mg_ons_nodesc.json'), 'w') as fp:
    json.dump(titles_mg, fp, indent = 4)

# Add description
for ix, t in enumerate(unit_group.loc[:, 'Minor']):
    if t in titles_mg.keys():
        titles_mg[t].append(unit_group.loc[ix, 'Description'])

# Write dictionary to pickle and json

#pickle.dump(titles_mg, open(os.path.join(lookup_dir, 'titles_minor_group_ons.pkl'), 'wb'))

with open(os.path.join(lookup_dir, 'titles_minor_group_ons.json'), 'w') as fp:
    json.dump(titles_mg, fp, indent = 4)
    
# Create dataframe with 'buckets' of titles

mg_buckets = pd.DataFrame.from_dict(titles_mg.items())
mg_buckets.columns = ['SOC_code', 'Titles']

# Combine elements of list into one string (i.e. bucket)
mg_buckets['Titles_joined']=mg_buckets['Titles'].apply(lambda x: ' '.join(x))

# Remove punctuation
mg_buckets['Titles_nopunct']=mg_buckets['Titles_joined'].\
apply(lambda x: utils.replace_punctuation(x).strip())


# Lemmatise titles to convert from plural forms to singular
mg_buckets['Titles_lemm']=mg_buckets['Titles_nopunct'].\
apply(lambda x: utils.lemmatise(x.split()))
mg_buckets['Titles_joined2']=mg_buckets['Titles_lemm'].\
apply(lambda x: ' '.join(x))
mg_buckets['Titles_nospace'] = mg_buckets['Titles_joined2'].\
apply(lambda x: re.sub(' +',' ',x))
mg_buckets_reduced = mg_buckets[['SOC_code', 'Titles_nospace']]

# Write dataframe to pickle and json

#mg_buckets_reduced.to_pickle(os.path.join(lookup_dir, 'mg_buckets_ons_df_processed.pkl'))
mg_buckets_reduced.to_json(os.path.join(lookup_dir, 'mg_buckets_ons_df_processed.json'), 
                           orient = 'records')