from __future__ import division
import pandas as pd
from sklearn import preprocessing

df = pd.read_csv('../data/amazon/ltid_project/isbn_all', header = None,
        delimiter = ' ')
df = df.sort_values(1, ascending = False)
subset = 250000
rels = df[2][df[2] > 0.01].size
dfs = df[:subset]
tp = dfs[dfs[2] > 0][0].count()
print('pre: %.2f' % (tp / subset))
print('rec: %.2f' % (tp / rels))
print('===')

'''
df = pd.read_csv('../data/amazon/julian_anal/isbn_major', header = None,
        delimiter = ' ')
print(df.columns)
rel = 6
cols = [1, 2, 3, 4, 5]
rels = df[rel][df[rel] > 0].size
subset = 250000
for k in cols:
    print('k = %d' % k)
    df[k] = (df[k] - df[k].min())/(df[k].max() - df[k].min())
    print('corr: %.2f' % df[k].corr(df[rel]))
    dfs = df.sort_values(k, ascending = False)[:subset]
    tp = dfs[(dfs[rel] > 0)][0].count()
    print('pre: %.2f' % (tp / subset))
    print('rec: %.2f' % (tp / rels))
    print('===')

df[7] = 0.46 * df[2] + 0.43 * df[4] + 0.01 * df[5]
dfs = df.sort_values(7, ascending = False)[:subset]
tp = dfs[(dfs[rel] > 0)][0].count()
print('pre: %.2f' % (tp / subset))
print('rec: %.2f' % (tp / rels))
print('===')
df[8] = (df[2] + df[4])/2
dfs = df.sort_values(8, ascending = False)[:subset]
tp = dfs[(dfs[rel] > 0)][0].count()
print('pre: %.2f' % (tp / subset))
print('rec: %.2f' % (tp / rels))
print('===')
final_ind = 8
df[final_ind] = df[final_ind].apply(lambda x : "{:.2f}".format(x))
df = df.sort_values(final_ind, ascending = False)[:subset]
df.to_csv('../data/amazon/isbn_final', columns=[0,final_ind], header=False,
        index=False)

isbn_rel = pd.read_csv('../data/amazon/julian_anal/isbn_qrels', header = None,
        delimiter = ' ')
isbn_rate = pd.read_csv('../data/amazon/isbn_rateavg_ratecount.csv', header = None)
df = pd.merge(isbn_rate, isbn_rel, on=0, how = 'left')
df.columns = [0, 1, 2, 3]

for k in [50, 60, 70, 80, 90]:
    print('k = %d' % k)
    tp = df[((df[2] > k) & (df[3] > 0))][0].count()
    tpfp = df[(df[2] > k)][0].count()
    tpfn = df[(df[3] > 0)][0].count()
    print(tp, tpfp, tpfn)
    print('pre: %.2f' % (tp / tpfp))
    print('rec: %.2f' % (tp / tpfn))
print('===')
df[1] = (df[1] - df[1].min())/(df[1].max() - df[1].min())
df[2] = (df[2] - df[2].min())/(df[2].max() - df[2].min())
df[4] = df[[1, 2]].mean(axis=1)
df4 = df.sort_values(4, ascending=False)
df4.to_csv('isbn_new.csv', columns=[0, 4], index = False, header = False)
for k in [0.1, 0.3, 0.5, 0.6, 0.9]:
    print('k = %d' % k)
    tp = df[((df[4] > k) & (df[3] > 0))][0].count()
    tpfp = df[(df[4] > k)][0].count()
    tpfn = df[(df[3] > 0)][0].count()
    print(tp, tpfp, tpfn)
    print('pre: %.2f' % (tp / tpfp))
    print('rec: %.2f' % (tp / tpfn))
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
'''
