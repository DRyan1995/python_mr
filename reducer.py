#!/usr/bin/env python
"""reducer.py"""

from operator import itemgetter
import sys
import json

current_word = None
# current_count = 0
word = None

#output_result_obj {"keyvalue": stats}
output_result_obj = {}
stats = {'body':{'frequency': 0, 'location':[]}, 'title':{'frequency': 0, 'location':[]}} #{"body/title":{frequency:, "location":[]}}

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    try:
        obj = json.loads(line) # obj here {"word": word, "body_loc/title_loc": }
    except ValueError as e:
        continue

    if 'word' not in obj or ('body_loc' not in obj and 'title_loc' not in obj):
        continue
    current_word = obj['word']

    # parse the input we got from mapper.py
    # word, count = line.split('\t', 1)

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if len(output_result_obj.keys()) == 0:
        output_result_obj[current_word] = stats
    if current_word == word:
        # current_count += count
        if 'body_loc' in obj:
            output_result_obj['body']['frequency'] += 1
            output_result_obj['body']['location'].append(body_loc)
        if 'title_loc' in obj:
            output_result_obj['title']['frequency'] += 1
            output_result_obj['title']['location'].append(body_loc)

    else:
        if current_word:
            pass
            # write result to STDOUT
            # print '%s\t%s' % (current_word, current_count)
        # current_count = count
        current_word = word

# do not forget to output the last word if needed!
if current_word == word:
    # print '%s\t%s' % (current_word, current_count)
    print json.dumps(output_result_obj)
    print "\n"
