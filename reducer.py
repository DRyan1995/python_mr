#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""reducer.py"""

from operator import itemgetter
import sys
import json

current_word = None
# current_count = 0

#output_result_obj {"url": stats}
output_result_obj = {}
stats = {'body':{'frequency': 0, 'location':[]}, 'title':{'frequency': 0, 'location':[]}} #{"body/title":{frequency:, "location":[]}}

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    # line = line.strip()
    if len(line.split('\t')) != 2:
        continue
    try:
        obj = json.loads(line.split('\t')[1]) # obj here {"word": word, "body_loc/title_loc":, "url":url }
    except ValueError as e:
        continue

    if 'word' not in obj or 'url' not in obj or ('body_loc' not in obj and 'title_loc' not in obj):
        print obj
        continue
    url = obj['url']
    word = obj['word']
    if not current_word:
        current_word = obj['word']
        stats = {'body':{'frequency': 0, 'location':[]}, 'title':{'frequency': 0, 'location':[]}} #{"body/title":{frequency:, "location":[]}}
        output_result_obj[url] = stats
    # parse the input we got from mapper.py
    # word, count = line.split('\t', 1)

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_word == word:
        if url not in output_result_obj:
            output_result_obj[url] = stats
        if 'body_loc' in obj and obj['body_loc'] not in output_result_obj[url]['body']['location']:
            output_result_obj[url]['body']['frequency'] += 1
            output_result_obj[url]['body']['location'].append(obj['body_loc'])
            output_result_obj[url]['body']['location'] = sorted(output_result_obj[url]['body']['location'])
        if 'title_loc' in obj and obj['title_loc'] not in output_result_obj[url]['title']['location']:
            output_result_obj[url]['title']['frequency'] += 1
            output_result_obj[url]['title']['location'].append(obj['title_loc'])
            output_result_obj[url]['title']['location'] = sorted(output_result_obj[url]['title']['location'])
    else:
        print json.dumps({current_word:output_result_obj})
        print "\n"
        current_word = word
        stats = {'body':{'frequency': 0, 'location':[]}, 'title':{'frequency': 0, 'location':[]}} #{"body/title":{frequency:, "location":[]}}
        output_result_obj = {}
        output_result_obj[url] = stats
        if 'body_loc' in obj:
            output_result_obj[url]['body']['frequency'] += 1
            output_result_obj[url]['body']['location'].append(obj['body_loc'])
        if 'title_loc' in obj:
            output_result_obj[url]['title']['frequency'] += 1
            output_result_obj[url]['title']['location'].append(obj['title_loc'])
