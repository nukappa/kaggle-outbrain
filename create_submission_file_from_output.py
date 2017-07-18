#!/usr/bin/env python3

import pandas as pd
import numpy as np

test_file = '../raw/clicks_test.csv.gz'
output_file = 'libffm/output'

print ('reading the test file...')
# read the test file
clicked = pd.read_csv(test_file)

print ('reading the predictions...') 
preds = pd.read_table(output_file, header=None)

# add it to the data frame
clicked['preds'] = preds

print ('sorting data frame by predictions...')
clicked.sort_values(by=['display_id', 'preds'], ascending=[True, False], inplace=True)

print ('splitting data frame by display_id...')
cg = clicked.groupby('display_id')

print ('generating predictions... [takes long time]')
predictions = [cg.get_group(x)['ad_id'].tolist() for x in sorted(cg.groups.keys())]

# unique display ids
disp_ids = np.unique(clicked['display_id'])

with open('new_submission.csv', 'w') as f:
    f.write('display_id,ad_id' + '\n')
    for row in range(len(predictions)):
        f.write(str(disp_ids[row]) + ',')
        for item in predictions[row]:
            f.write(str(item) + ' ')
        f.write('\n')
