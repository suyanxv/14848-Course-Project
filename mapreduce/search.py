#!/usr/bin/env python

import sys

with open('test_results','r') as f:
    for line in f:
        first_word = line.split(None, 1)[0]
        if sys.argv[1] == first_word:
            print(line)