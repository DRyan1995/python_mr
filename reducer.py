#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""reducer.py"""

from operator import itemgetter
import sys
import json

current_word = None
# current_count = 0

#output_result_obj {"keyvalue": stats}
output_result_obj = {}
stats = {'body':{'frequency': 0, 'location':[]}, 'title':{'frequency': 0, 'location':[]}} #{"body/title":{frequency:, "location":[]}}

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    # line = line.strip()
    if len(line.split()) != 2:
        continue
    try:
        obj = json.loads(line.split(' ')[1]) # obj here {"word": word, "body_loc/title_loc": }
    except ValueError as e:
        continue

    if 'word' not in obj or ('body_loc' not in obj and 'title_loc' not in obj):
        continue
    if not current_word:
        current_word = obj['word']
        output_result_obj[current_word] = stats
    word = obj['word']

    # parse the input we got from mapper.py
    # word, count = line.split('\t', 1)

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_word == word:
        if 'body_loc' in obj:
            output_result_obj['body']['frequency'] += 1
            output_result_obj['body']['location'].append(obj['body_loc'])
        if 'title_loc' in obj:
            output_result_obj['title']['frequency'] += 1
            output_result_obj['title']['location'].append(obj['title_loc'])
    else:
        current_word = word
        output_result_obj = {}
        output_result_obj[word] = stats
        print json.dumps(output_result_obj)
        print "\n"
            # print current_word
            # write result to STDOUT
            # print '%s\t%s' % (current_word, current_count)
        # current_count = count
        current_word = word

# do not forget to output the last word if needed!
if current_word == word:
    # print '%s\t%s' % (current_word, current_count)
    print json.dumps(output_result_obj)
    print "\n"
