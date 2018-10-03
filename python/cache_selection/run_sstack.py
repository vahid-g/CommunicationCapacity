'''
Samples freq into test_freq and train_freq arrays and uses them to
partition the test and train data. The rest is similar to run_swiki. Note that
in this approach, a query may appear in both train and test data
'''

from __future__ import division
import sys
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from sklearn import linear_model
from sklearn.ensemble import RandomForestClassifier
from utils import print_results
from stack_anal import analyze
import datetime
import argparse
from run_swiki import train_lr

def sample_viewcount(view_count, size):
    print('sampling..')
    p = view_count / view_count.sum()
    s = np.random.choice(a=view_count.size, size=size, p=p.astype(float))
    test_count = pd.Series(np.zeros(view_count.size, dtype=int), index=view_count.index)
    for i in s:
        test_count[i] += 1
    return test_count


def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('filename', help="input features file")
    parser.add_argument('-s', '--split', type=float, help="test split ratio", default=0.33)
    parser.add_argument('-t', '--threshold', type=float, help="decision boundry", default=0.5)
    parser.add_argument('-o', '--output', action='store_true', help="save the output")
    args = parser.parse_args()
    filename = args.filename
    test_size = args.split
    t = args.threshold
    write_output = args.output
    print('running swiki with test size %.2f and threshold %.2f' %
          (test_size, t))
    df = pd.read_csv("../../data/cache_selection_structured/" + filename)
    print("df size: " + str(df.shape))
    df = df.fillna(0)
    df = df.T.drop_duplicates().T
    print("df size after dedup: " + str(df.shape))
    labels = np.where(df['full'] > df['cache'], 1, 0)
    viewcount = df['freq']
    print("bad queries ratio (with freq): %.2f" %
          (100 * np.sum(labels * viewcount) / viewcount.sum()))
    test_viewcount = sample_viewcount(viewcount, int(test_size *
                                                     viewcount.sum()))
    train_viewcount = viewcount - test_viewcount
    X = df[train_viewcount > 0].copy()
    y = labels[train_viewcount > 0].copy()
    X_test = df[test_viewcount > 0].copy()
    y_test = labels[test_viewcount > 0].copy()
    X = X.drop(['query', 'freq', 'cache', 'full'], axis=1)
    test_queries = X_test['query']
    test_freq = test_viewcount
    subset_mrr = X_test['cache']
    db_mrr = X_test['full']
    X_test = X_test.drop(['query', 'freq', 'cache', 'full'], axis=1)
    #print(df.corr()['label'].sort_values())
    print("train set size, bad queries and bad query ratio: %d, %d, %.2f"
          % (train_viewcount.sum(), (labels * train_viewcount).sum(),
             (100 * (labels * train_viewcount).sum() / train_viewcount.sum())))
    print("test set size, bad queries and bad query ratio: %d, %d, %.2f"
          % (test_viewcount.sum(), (labels * test_viewcount).sum(),
             (100 * (labels * test_viewcount).sum() / test_viewcount.sum())))
    # learn the model
    # y_pred = train_lr(X, y, X_test, y_test, t, df.columns.values[2:-2])
    y_pred = train_lr(X, y, X_test, y_test, t)
    output = pd.DataFrame()
    output['query'] = test_queries
    output['TestFreq'] = test_freq
    output['cache'] = subset_mrr
    output['full'] = db_mrr
    output['Label'] = y_test
    output['ql_label'] = X_test['ql_0_0'] < X_test['ql_rest_0_0']
    output['ql'] = np.where(output['ql_label'] == 1, db_mrr, subset_mrr)
    output['ml_label'] = pd.Series(y_pred, index=output.index)
    output['ml'] = np.where(output['ml_label'] == 1, db_mrr, subset_mrr)
    output['best'] = np.maximum(subset_mrr, db_mrr)
    r = np.random.randint(0, 2, output['cache'].size)
    output['rand'] = np.where(r == 1, output['full'], output['cache'])
    analyze(output, 'cache', 'full','TestFreq')
    if (write_output):
        output.to_csv('%s%s_result.csv' % ('../../data/cache_selection_structured/',
                                       filename[:-4]), index=False)

if __name__ == "__main__":
    main(sys.argv[1:])
