#!/usr/bin/env python3

from methods import *

import pandas as pd
import sys

# command-line argument separates cv from mapk
setting = sys.argv[1]

params = ''
if len(sys.argv) > 2:
    params = '_' + sys.argv[2]

print ('reading test file for', setting, '...')
test_file = '../raw/for_' + setting + '/clicks_test.csv.gz'
output_file ='libffm/for_' + setting + '/output' + params

# read the test file
clicked = pd.read_csv(test_file)

print ('computing actual clicks ...')
# these are the acual clicks
actual = clicked.query('clicked == 1')['ad_id'].tolist()
actual = [[x] for x in actual]

print ('reading the predictions ...')
# read the predictions
preds = pd.read_table(output_file, header=None)

# add it to the data frame
clicked['preds'] = preds

print ('sorting data frame by predictions ...')
# sort data frame by prediction values
clicked.sort_values(by=['display_id', 'preds'], ascending=[True, False], inplace=True)

print ('splitting data frame by display_id')
# split it by display_id
cg = clicked.groupby('display_id')

print ('generating predictions ... [takes long]')
# this is the list of predictions
predicted = [cg.get_group(x)['ad_id'].tolist() for x in sorted(cg.groups.keys())]

print ('calculating MAPK@12')
print (round(mapk(actual, predicted, k=12), 5))
