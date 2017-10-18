from __future__ import division
import pandas as pd
'''
isbn_rel = pd.read_csv('../data/amazon/analysis/isbn_qrels', header = None,
        delimiter = ' ')
isbn_rate = pd.read_csv('../data/amazon/isbn_ratecount.csv', header = None)
df = pd.merge(isbn_rate, isbn_rel, on=0, how = 'left')
df.columns = [0, 1, 2]
for k in [0, 1, 10, 50, 95]:
    print('k = %d' % k)
    tp = df[((df[1] > k) & (df[2] > 0))][0].count()
    tpfp = df[(df[1] > k)][0].count()
    tpfn = df[(df[2] > 0)][0].count()
    print(tp, tpfp, tpfn)
    print('pre: %.2f' % (tp / tpfp))
    print('rec: %.2f' % (tp / tpfn))

isbn_rel = pd.read_csv('../data/amazon/analysis/isbn_qrels', header = None,
        delimiter = ' ')
isbn_rate = pd.read_csv('../data/amazon/isbn_reviewcount.csv', header = None)
df = pd.merge(isbn_rate, isbn_rel, on=0, how = 'left')
df.columns = [0, 1, 2]
for k in [0, 1, 10, 50, 95]:
    print('k = %d' % k)
    tp = df[((df[1] > k) & (df[2] > 0))][0].count()
    tpfp = df[(df[1] > k)][0].count()
    tpfn = df[(df[2] > 0)][0].count()
    print(tp, tpfp, tpfn)
    print('pre: %.2f' % (tp / tpfp))
    print('rec: %.2f' % (tp / tpfn))
'''

isbn_rel = pd.read_csv('../data/amazon/julian_anal/isbn_qrels', header = None,
        delimiter = ' ')
isbn_rate = pd.read_csv('../data/amazon/isbn_rateavg_ratecount.csv', header = None)
df = pd.merge(isbn_rate, isbn_rel, on=0, how = 'left')
df.columns = [0, 1, 2, 3]
'''
for k in [50, 60, 70, 80, 90]:
    print('k = %d' % k)
    tp = df[((df[2] > k) & (df[3] > 0))][0].count()
    tpfp = df[(df[2] > k)][0].count()
    tpfn = df[(df[3] > 0)][0].count()
    print(tp, tpfp, tpfn)
    print('pre: %.2f' % (tp / tpfp))
    print('rec: %.2f' % (tp / tpfn))
print('===')
'''
df[1] = (df[1] - df[1].min())/(df[1].max() - df[1].min())
df[2] = (df[2] - df[2].min())/(df[2].max() - df[2].min())
df[4] = df[[1, 2]].mean(axis=1)
df4 = df.sort_values(4, ascending=False)
df4.to_csv('isbn_new.csv', columns=[0, 4], index = False, header = False)
'''
for k in [0.1, 0.3, 0.5, 0.6, 0.9]:
    print('k = %d' % k)
    tp = df[((df[4] > k) & (df[3] > 0))][0].count()
    tpfp = df[(df[4] > k)][0].count()
    tpfn = df[(df[3] > 0)][0].count()
    print(tp, tpfp, tpfn)
    print('pre: %.2f' % (tp / tpfp))
    print('rec: %.2f' % (tp / tpfn))
'''
k = 54000
df4 = df.sort_values(4, ascending=False)
df4 = df4.iloc[:k]
tp = df4[df4[3] > 0][0].count()
print(tp)
df1 = df.sort_values(1, ascending=False)
df1 = df1.iloc[:k]
tp = df1[(df1[3] > 0)][0].count()
print(tp)
df2 = df.sort_values(2, ascending=False)
df2 = df2.iloc[:k]
tp = df2[(df2[3] > 0)][0].count()
print(tp)

