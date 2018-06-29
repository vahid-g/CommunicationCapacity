import numpy as np
import random
import bisect

pdf = np.genfromtxt('/scratch/data-sets/wikipedia/counts13.csv', delimiter=',')
s = np.sum(pdf)
pdf = pdf / s
cdf = list()
s = 0
for x in pdf:
    s += x
    cdf.append(s)

d = dict()
for k in range(1496826384):
    r = random.random()
    i = bisect.bisect_left(cdf, r)
    if i in d:
        d[i] += 1
    else:
        d[i] = 1

