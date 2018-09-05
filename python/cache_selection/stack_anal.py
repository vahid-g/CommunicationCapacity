from __future__ import division
import pandas as pd
import sys
from scipy.stats import ttest_rel
from scipy.stats import ttest_ind

def main(argv):
    f = argv[0]
    c = pd.read_csv('../../data/python_data/' + f)
    analyze(c, '18', '100', 'TestViewCount')

def analyze(c, subset, db, popularity):
    query_count = c.shape[0]
    print('distinct query count: %d' % query_count)
    if popularity not in c:
        print('warning: popularity column not found')
        c[popularity] = 1
    s = c[popularity].sum()
    print('query count: %d' % c[popularity].sum())
    r1 = c[subset] * c[popularity]
    r2 = c[db] * c[popularity]
    r3 = c['ql'] * c[popularity]
    r4 = c['ml'] * c[popularity]
    r5 = c['best'] * c[popularity]
    r6 = c['rand'] * c[popularity]
    s = c[popularity].sum()
    print('set \t sub \t db \t ql \t ml \t best \t rand')
    print('all \t %.2f \t %.2f \t %.2f \t %.2f \t %.2f \t %.2f' %
          (r1.sum() / s, r2.sum() / s, r3.sum() / s, r4.sum() / s, r5.sum() /
           s, r6.sum() / s))
    b = c['Label'] == 1
    s = c[popularity][b].sum()
    r1 = c[subset][b] * c[popularity][b]
    r2 = c[db][b] * c[popularity][b]
    r3 = c['ql'][b] * c[popularity][b]
    r4 = c['ml'][b] * c[popularity][b]
    r5 = c['best'][b] * c[popularity][b]
    r6 = c['rand'][b] * c[popularity][b]
    sdf= c[popularity][b].sum()
    print('bad \t %.2f \t %.2f \t %.2f \t %.2f \t %.2f \t %.2f' %
          (r1.sum() / s, r2.sum() / s, r3.sum() / s, r4.sum() / s, r5.sum() /
           s, r6.sum() / s))
    nb = c['Label'] == 0
    s = c[popularity][nb].sum()
    r1 = c[subset][nb] * c[popularity][nb]
    r2 = c[db][nb] * c[popularity][nb]
    r3 = c['ql'][nb] * c[popularity][nb]
    r4 = c['ml'][nb] * c[popularity][nb]
    r5 = c['best'][nb] * c[popularity][nb]
    r6 = c['rand'][nb] * c[popularity][nb]
    sdf= c[popularity][nb].sum()
    print('n_bad \t %.2f \t %.2f \t %.2f \t %.2f \t %.2f \t %.2f' %
          (r1.sum() / s, r2.sum() / s, r3.sum() / s, r4.sum() / s, r5.sum() /
           s, r6.sum() / s))
    bad_count = b.value_counts()[True]
    print('%d distinct bad queries (%.2f %%)' %
          (bad_count, bad_count * 100 / query_count))
    ml_to_cache = c['ml_label'] * c[popularity]
    ql_to_cache = c['ql_label'] * c[popularity]
    s = float(c[popularity].sum())
    print('queries sent to full db by ml: %.2f%%' % (ml_to_cache.sum() / s))
    print('queries sent to full db by ql: %.2f%%' % (ql_to_cache.sum() / s))
    print('queries with mrr > 0 on cache: %.2f%%' %
          (c[popularity][c[subset] > 0].sum() / c[popularity].sum()))
    print('queries with mrr > 0 on cache: %.2f%%' %
          (c[popularity][c[db] > 0].sum() / c[popularity].sum()))
    print('ml and rand ' + str(ttest_rel(c['ml'], c['rand'])))
    print('ql and rand ' + str(ttest_rel(c['ql'], c['rand'])))
    print('subset and rand ' + str(ttest_rel(c[subset], c['rand'])))

if __name__ == "__main__":
    main(sys.argv[1:])

