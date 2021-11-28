#!/usr/bin/env python

import csv
import operator
import sys

sample = open("test_results")
csv1 = csv.reader(sample, delimiter='\t')
sort = sorted(csv1, key=lambda x : int(x[2]), reverse=True)
n = int(sys.argv[1])
i = 0
for line in sort:
    print('\t'.join(line))
    i += 1
    if i >= n:
        break
