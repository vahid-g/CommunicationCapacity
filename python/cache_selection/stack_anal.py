import pandas as pd
import sys

f = sys.argv[1]
c = pd.read_csv('../../data/python_data/' + f)
zero_count = c['pred'].value_counts()[0]
query_count = c.shape[0]
print('distinct query count: %d' % query_count)
print('%d queries submitted to the subset (%.2f %%)' %
      (zero_count, zero_count * 100 / query_count))
print('average mrr:')
s = c['viewcount'].sum()
r1 = c['15'] * c['viewcount']
r2 = c['100'] * c['viewcount']
r3 = c['ml'] * c['viewcount']
r4 = c['best'] * c['viewcount']
s = c['viewcount'].sum()
print('subset = %.2f \t db = %.2f \t ml = %.2f \t best = %.2f ' %
      (r1.sum() / s, r2.sum() / s, r3.sum() / s, r4.sum() / s))

b = c['15'] < c['100']
bad_count = b.value_counts()[True]
print('%d distinct bad queries (%.2f %%)' %
      (bad_count, bad_count * 100 / query_count))
print('average mrr for bad queries:')
r1 = c['15'][b] * c['viewcount']
r2 = c['100'][b] * c['viewcount']
r3 = c['ml'][b] * c['viewcount']
r4 = c['best'][b] * c['viewcount']
s = c['viewcount'][b].sum()
print('subset = %.2f \t db = %.2f \t ml = %.2f \t best = %.2f ' %
      (r1.sum() / s, r2.sum() / s, r3.sum() / s, r4.sum() / s))

