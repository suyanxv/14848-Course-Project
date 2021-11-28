#!/usr/bin/env python

import sys

current_word = None
current_count = 0
current_posting = []
current_dict = dict()

for line in sys.stdin:
    line = line.strip()
    word, posting = line.split('\t', 1)
    doc_folder, doc_name = posting.split('/', 1)
    doc_name, count = doc_name.split(':', 1)
    try:
        count = int(count)
    except ValueError:
        continue
    
    if current_word == word:
        current_count += count # total count
        current_posting.append(posting) # append posting
        dir = doc_folder+'/'+doc_name
        if dir in current_dict:
            current_dict[dir] += 1
        else:
            current_dict[dir] = 1
    else:
        if current_word:
            print('%s\t%s\t%d' % (current_word, current_dict, current_count))
            # sort posting
        current_word = word
        current_count = count
        current_posting = [posting]
        current_dict = dict()
        dir = doc_folder+'/'+doc_name
        current_dict[dir] = 1
    
if current_word:
    print('%s\t%s\t%d' % (current_word, current_dict, current_count))