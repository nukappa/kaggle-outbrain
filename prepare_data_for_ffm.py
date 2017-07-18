#!/usr/bin/env python3

import gzip
import csv
from collections import defaultdict
import sys
import time
import numpy as np
from math import floor

prefix = ''
if len(sys.argv) > 1:
    prefix = 'for_' + sys.argv[1] + '/'


###
###
### Raw features
###

def read_promoted_content_as_dict():
    """Hashes the promoted content into a dictionary form
       so that target_document_id, campaign_id and advertiser_id are
       retrieved quickly"""
    promoted_content_dict = defaultdict(list)
    with gzip.open('../raw/promoted_content.csv.gz', 'rt') as pc:
        pc_reader = csv.reader(pc, delimiter=',')
        next(pc_reader)
        for row in pc_reader:
            row = list(map(int, row))
            promoted_content_dict[row[0]] = row[1:]
    return (promoted_content_dict)

def read_events_as_dict():
    events_dict = defaultdict(list)
    with gzip.open('../raw/' + prefix + 'events.csv.gz', 'rt') as ev:
        ev_reader = csv.reader(ev, delimiter=',')
        next(ev_reader)
        for row in ev_reader:
            # doc_id, platform, uuid, timestamp
            events_dict[int(row[0])] = [int(row[2]), row[4], row[1], int(row[3]), row[5][:2]]
    return (events_dict)

def read_documents_categories():
    """Hashes the file to use it later."""
    doc_cat_dict = defaultdict(list)
    with gzip.open('../raw/documents_categories.csv.gz', 'rt') as dc:
        dc_reader = csv.reader(dc, delimiter=',')
        next(dc_reader)
        for row in dc_reader:
            if int(row[0]) not in doc_cat_dict:
                doc_cat_dict[int(row[0])] = [[int(row[1])], [float(row[2])]]
            else:
                doc_cat_dict[int(row[0])][0].append(int(row[1]))
                doc_cat_dict[int(row[0])][1].append(float(row[2]))
    return (doc_cat_dict)

def read_documents_meta():
    """Hashes the file for later use."""
    doc_meta_dict = defaultdict(list)
    with gzip.open('../raw/documents_meta.csv.gz', 'rt') as dm:
        dm_reader = csv.reader(dm, delimiter=',')
        next(dm_reader)
        for row in dm_reader:
            doc_meta_dict[int(row[0])] = [row[1], row[2]]
    return (doc_meta_dict)

def read_documents_topics():
    doc_topics_dict = defaultdict(list)
    with gzip.open('../raw/documents_topics.csv.gz', 'rt') as dt:
        dt_reader = csv.reader(dt, delimiter=',')
        next(dt_reader)
        for row in dt_reader:
            if int(row[0]) not in doc_topics_dict:
                doc_topics_dict[int(row[0])] = [[] , []]
            doc_topics_dict[int(row[0])][0].append(row[1])
            doc_topics_dict[int(row[0])][1].append(float(row[2]))
    return (doc_topics_dict)

def read_documents_entities():
    doc_entities_dict = defaultdict(list)
    with gzip.open('../raw/documents_entities_new.csv.gz', 'rt') as de:
        de_reader = csv.reader(de, delimiter=',')
        next(de_reader)
        for row in de_reader:
            if int(row[0]) not in doc_entities_dict:
                doc_entities_dict[int(row[0])] = [[], []]
            doc_entities_dict[int(row[0])][0].append(int(row[1]))
            doc_entities_dict[int(row[0])][1].append(float(row[2]))
    return (doc_entities_dict)

def clean_dictionaries_from_NA(doc_meta_dict):
    """Cleans the dictionaries from NAs. Basically replaces missing
       values with zero (which can be treated as their own in FFM)."""
    for key in doc_meta_dict:
        if doc_meta_dict[key][0] == '':
            doc_meta_dict[key][0] = '0'
        if doc_meta_dict[key][1] == '':
            doc_meta_dict[key][1] = '0'

###
### Engineered features from here below
###

def read_leak():
    leak_dict = defaultdict(dict)
    with gzip.open('../processed/leak.csv.gz', 'rt') as f:
        next(f)
        for line in f:
             leak_dict[int(line.split(',')[0])] = {i:i for i in line.split(',')[1].strip().split(' ')}
    return (leak_dict)

def read_num_ads_per_display():
    """"Reads previously extracted file with number of ads per display ad dictionary."""
    num_ads = {}
    with gzip.open('../processed/' + prefix + 'num_ads_per_display.csv.gz', 'rt') as f:
        f_reader = csv.reader(f, delimiter=',')
        next(f_reader)
        for row in f_reader:
            row = list(map(int, row))
            num_ads[row[0]] = row[1]
    return (num_ads)

def read_ad_frequencies():
    """How many times does the ad_id occur? Includes data_test information."""
    ad_freqs = {}
    with gzip.open('../processed/' + prefix + 'ad_freqs.csv.gz', 'rt') as f:
      f_reader = csv.reader(f, delimiter=',')
      next(f_reader)
      for row in f_reader:
          row = list(map(int, row))
          ad_freqs[row[0]] = row[1]
    return (ad_freqs)

def read_which_ads_per_display(filename, display_ads_dict={}):
    if display_ads_dict == {}:
        display_ads_dict = defaultdict(set)
    with gzip.open(filename, 'rt') as f:
        f_reader = csv.reader(f, delimiter=',')
        next(f_reader)
        for row in f_reader:
            row = list(map(int, row))
            if row[0] not in display_ads_dict:
                display_ads_dict[row[0]] = set()
            display_ads_dict[row[0]].add(row[1])
    return (display_ads_dict)

def read_ad_clicks(events_dict):
    clicked = defaultdict(dict)
    with gzip.open('../raw/' + prefix + 'clicks_train.csv.gz', 'rt') as f:
        f_reader = csv.reader(f, delimiter=',')
        next(f_reader)
        idx = 0
        current_interval = 0
        for row in f_reader:
            if idx % 1000000 == 0:
                print (idx, 'lines read')
            row = list(map(int, row))

            timestamp = events_dict[row[0]][3] / (3600 * 1000)
            day = floor(timestamp / 24)
            hour = floor(timestamp % 24)
            interval = 24*day + hour

            if interval > current_interval:
                current_interval = interval
                for ad in clicked:
                    clicked[ad][interval][0] = clicked[ad][interval-1][0]
                    clicked[ad][interval][1] = clicked[ad][interval-1][1]

            if row[1] not in clicked:
                clicked[row[1]] = {i : [0, 0] for i in range(24 * 15)}

            clicked[row[1]][interval][row[2]] += 1
            idx += 1
    return (clicked)


### Prepare the data for libFFM
def prepare_clicks_file(filename, is_train=True, 
                        pc_dict={}, events_dict={}, doc_cat_dict={}, doc_meta_dict={},
                        ad_clicks_dict={}, doc_topics_dict={}, leak_dict={},
                        num_ads_dict={}, ad_freqs_dict={},
                        uuid_main_cat_dict={}, td_cat_freqs_dict={},
                        display_ads_dict={}, doc_entities_dict={}):
    """Reads the clicks_train/test file and by using the prom_content dictionary
       outputs an FFM format file with the following features
       0: leak
       1: campaign_id from promoted_content
       2: advertiser_id from promoted_content
       3: platform from events # definitely helps
       4: target_document_id from promoted_content
       5: document_id from events
       6: target_doc categories from documents_categories
       7: doc top categories from documents_categories
       8: document_id source_id from documents_meta
       9: ads present in the display
       10: source_id of current ad_id in question
       11: source_id of all ads in the display
       12: common categories between target_doc and doc
       """
    if is_train:
        file_type = 'train'
    else:
        file_type = 'test'
    with gzip.open(filename, 'rt') as f, open('libffm/' + prefix + 'data_to_' + file_type + '.txt', 'w') as fo:
        f_reader = csv.reader(f, delimiter=',')
        next(f_reader)
        for row in f_reader:
            row = list(map(int, row))
            if is_train:
                label = str(row[2])
            else:
                label = '0'

            target_doc_id = pc_dict[row[1]][0]
            doc_id = events_dict[row[0]][0]
            
            fo.write(label)

            fo.write(' 0:1:' + str((target_doc_id in leak_dict) * (events_dict[row[0]][2] in leak_dict[target_doc_id])))
            fo.write(' 1:' + str(pc_dict[row[1]][1]) + ':1') # campaign_id
            fo.write(' 2:' + str(pc_dict[row[1]][2]) + ':1') # advertiser_id
            fo.write(' 3:' + str(events_dict[row[0]][1]) + ':1')
            fo.write(' 4:' + str(target_doc_id) + ':1')
            fo.write(' 5:' + str(doc_id) + ':1')

            if target_doc_id in doc_cat_dict:
                for cat in range(len(doc_cat_dict[target_doc_id][0])):
                    if doc_cat_dict[target_doc_id][1][cat] > 0.05:
                        fo.write(' 6:' + str(doc_cat_dict[target_doc_id][0][cat]) + ':'
                                 + str(round(doc_cat_dict[target_doc_id][1][cat], 2)))

            if doc_id in doc_cat_dict:
                for cat in range(len(doc_cat_dict[doc_id][0])):
                    if doc_cat_dict[doc_id][1][cat] > 0.05:
                        fo.write(' 7:' + str(doc_cat_dict[doc_id][0][cat]) + ':'
                                 + str(round(doc_cat_dict[doc_id][1][cat], 2)))

            if doc_id in doc_meta_dict:
                fo.write(' 8:' + str(doc_meta_dict[doc_id][0]) + ':1')

            # Which other ads are there.
            for ad_id in display_ads_dict[row[0]]: 
                fo.write(' 9:' + str(ad_id) + ':' + str(round(1/(len(display_ads_dict[row[0]])), 2)))

            # The source_id of the ad in question.
            if target_doc_id in doc_meta_dict:
                fo.write(' 10:' + str(doc_meta_dict[target_doc_id][0]) + ':1')
            
            # Source_id for the ads in the display. 
            for ad_id in display_ads_dict[row[0]]-set([row[1]]):
                ad_target_doc_id = pc_dict[ad_id][0]
                if ad_target_doc_id in doc_meta_dict:
                    fo.write(' 11:' + str(doc_meta_dict[ad_target_doc_id][0]) + ':1')

            # Boolean: common categories between target and doc
            if target_doc_id in doc_cat_dict and doc_id in doc_cat_dict:
                targ_cats = doc_cat_dict[target_doc_id][0]
                doc_cats = doc_cat_dict[doc_id][0]
                common_cats = set(targ_cats).intersection(set(doc_cats))
                for cat in common_cats:
                    fo.write( ' 12:' + str(cat) + ':1')

            fo.write('\n')


if __name__ == '__main__':

    train_file = '../raw/' + prefix + 'clicks_train.csv.gz'
    test_file = '../raw/' + prefix + 'clicks_test.csv.gz'
   
    start_time = time.time()
    print ('reading documents entities as dictioanry ...')
    doc_entities_dict = read_documents_entities()
    print ('...', str(round(time.time()-start_time, 2)), 'seconds')
    
    start_time = time.time()
    print ('reading promoted content as dictionary ...')
    pc_dict = read_promoted_content_as_dict()
    print ('reading events as dictionary ...')
    events_dict = read_events_as_dict()
    print ('...', str(round(time.time()-start_time, 2)), 'seconds')
  
    print ('reading documents_categories as dictionary ...')
    start_time = time.time()
    doc_cat_dict = read_documents_categories()
    print ('...', str(round(time.time()-start_time, 2)), 'seconds')
    print ('reading documents_meta as dictionary ...')
    start_time = time.time()
    doc_meta_dict = read_documents_meta()
    print ('...', str(round(time.time()-start_time, 2)), 'seconds')
    print ('reading documents_topics as dictionary ...')
    doc_topics_dict = read_documents_topics()

    print ('cleaning dictionaries ...')
    clean_dictionaries_from_NA(doc_meta_dict)

    start_time = time.time()
    print ('reading ad_clicks as dictionary ...')
    ad_clicks_dict = read_ad_clicks(events_dict)
    print ('...', str(round(time.time()-start_time, 2)), 'seconds')

    print ('reading number of ads per display ...')
    num_ads_dict = read_num_ads_per_display()
    print ('reading ad_id frequencies as dictionary ...')
    ad_freqs_dict = read_ad_frequencies()

    print ('reading display_ads_dictionary ...')
    start_time = time.time()
    display_ads_dict = read_which_ads_per_display(train_file)
    display_ads_dict = read_which_ads_per_display(test_file, display_ads_dict=display_ads_dict)
    print ('...', str(round(time.time()-start_time, 2)), 'seconds')

    start_time = time.time()
    print ('reading leak ...')
    leak_dict = read_leak()
    print ('...', str(round(time.time()-start_time, 2)), 'seconds')

    print ('preparing train file ...')
    start_time = time.time()
    prepare_clicks_file(train_file, is_train=True, pc_dict=pc_dict,
                        events_dict=events_dict, leak_dict=leak_dict,
                        doc_cat_dict=doc_cat_dict, doc_meta_dict=doc_meta_dict,
                        display_ads_dict=display_ads_dict,
                        doc_topics_dict=doc_topics_dict,
                        num_ads_dict=num_ads_dict,
                        ad_clicks_dict=ad_clicks_dict,
                        doc_entities_dict=doc_entities_dict,
                        ad_freqs_dict=ad_freqs_dict)
    print ('...', str(round(time.time()-start_time, 2)), 'seconds')
    
    print ('preparing test ...')
    start_time = time.time()
    prepare_clicks_file(test_file, is_train=False, pc_dict=pc_dict,
                        events_dict=events_dict, leak_dict=leak_dict,
                        doc_cat_dict=doc_cat_dict, doc_meta_dict=doc_meta_dict,
                        display_ads_dict=display_ads_dict,
                        doc_topics_dict=doc_topics_dict,
                        num_ads_dict=num_ads_dict, ad_clicks_dict=ad_clicks_dict,
                        doc_entities_dict=doc_entities_dict,
                        ad_freqs_dict=ad_freqs_dict)
    print ('...', str(round(time.time()-start_time, 2)), 'seconds')
