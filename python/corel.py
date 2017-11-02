# measures the effectiveness of a subset in terms of precision and recall

from __future__ import division
import pandas as pd
from sklearn import preprocessing

# df: input dataframe with isbn and w (weight) columns
# subset: subset size
# qrel_ltids: a dataframe with one column that shows ltid of relevant tuples
# isbn_ltid: a dataframe with two colums isbn and ltid.
def order_acc(df, subset, qrel_ltids, isbn_ltid):
    df = df.sort_values('w', ascending = False)
    subset_size = int(round(df['isbn'].size * subset))
    ltids = pd.merge(df[:subset_size], isbn_ltid, on='isbn',
            how='inner')[['ltid']]
    ltids = ltids.drop_duplicates('ltid')
    intersect = pd.merge(ltids, qrel_ltids, on = 'ltid')
    tp = len(intersect.index)
    print('pre: %.2f \t rec: %.2f' % (tp / subset_size, tp /
        len(qrel_ltids.index)))

qrel_ltids = pd.read_csv('../data/amazon/queries/inex14sbs.qrels', header = None, delimiter = ' ')
qrel_ltids = qrel_ltids[[2, 3]]
qrel_ltids.columns = ['ltid', 'rel']
qrel_ltids = qrel_ltids[qrel_ltids['rel'] > 0.01]
qrel_ltids = qrel_ltids[['ltid']]

isbn_ltids = pd.read_csv('../data/amazon/queries/isbn_ltid', header = None,
    delimiter = ' ')
isbn_ltids.columns = ['isbn', 'ltid']

for subset in [0.02, 0.32]:
    '''
    df = pd.read_csv('../data/amazon/ltid_project/isbn_all', header = None,
        delimiter = ' ')
    df = df[[0, 1]]
    df.columns = ['isbn', 'w']
    order_acc(df, subset, qrel_ltids, isbn_ltids)
    '''

    df = pd.read_csv('../data/amazon/julian_anal/isbn_ratecomb', header = None,
        delimiter = ' ')
    df = df[[0, 1]]
    df.columns = ['isbn', 'w']
    order_acc(df, subset, qrel_ltids, isbn_ltids)

    df = pd.read_csv('../data/amazon/ltid_project/isbn_avgratecount', header = None,
        delimiter = ' ')
    df = df[[0, 1]]
    df.columns = ['isbn', 'w']
    order_acc(df, subset, qrel_ltids, isbn_ltids)

    df = pd.read_csv('../data/amazon/julian_anal/isbn_ratecountaverage', header = None,
        delimiter = ' ')
    df = df[[0, 1]]
    df.columns = ['isbn', 'w']
    order_acc(df, subset, qrel_ltids, isbn_ltids)

    print('===============================')
