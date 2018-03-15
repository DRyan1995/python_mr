#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""mapper.py"""
import sys
import re
import json


# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    # line = line.strip()
    try:
        obj = json.loads(line)
    except ValueError as e:
        continue

    # split the line into words
    if 'url' not in obj:
        continue

    if 'body' in obj:
        loc = -1
        words = obj['body'].split(' ')
        for word in words:
            url = obj['url']
            loc += 1
            try:
                word = word.decode("utf8")
                word = re.sub("[\s+\.\!\/_,$%^*(+\"\']+|[+——！，。？、~@#￥%……&*（）]+".decode("utf8"), "".decode("utf8"),word)
                word = re.sub(ur"[%s]+" %string.punctuation, "", word.decode("utf-8"))
                # word = word.lower()
            except UnicodeEncodeError as e:
                continue
            if len(word) == 0:
                continue
            print "{}\t{}\n".format(word, json.dumps({"word": word, "url": url, "body_loc": loc}))

    if 'title' in obj:
        loc = -1
        words = obj['title'].split(' ')
        for word in words:
            loc += 1
            try:
                word = word.decode("utf8")
                word = re.sub("[\s+\.\!\/_,$%^*(+\"\']+|[+——！，。？、~@#￥%……&*（）]+".decode("utf8"), "".decode("utf8"),word)
                word = re.sub(ur"[%s]+" %string.punctuation, "", word.decode("utf-8"))
                # word = word.lower()
            except UnicodeEncodeError as e:
                continue
            if len(word) == 0:
                continue
            print "{}\t{}\n".format(word, json.dumps({"word": word, "url": url, "title_loc": loc}))
    #
    # words = line.split()
    # # increase counters
    # for word in words:
    #     # write the results to STDOUT (standard output);
    #     # what we output here will be the input for the
    #     # Reduce step, i.e. the input for reducer.py
    #     #
    #     # tab-delimited; the trivial word count is 1
    #     print '%s\t%s' % (word, 1)
