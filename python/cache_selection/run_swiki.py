''' runs simple classification on cache selection problem '''
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
def train_lr( X, y, X_test, y_test, t, col_names = None, sample_weight = None ):
    sc = MinMaxScaler().fit(X)
    X = sc.transform(X)
    start = datetime.datetime.now()
    X_test_trans = sc.transform(X_test)
    print("training balanced LR..")
    lr = linear_model.LogisticRegression(class_weight='balanced')
    if sample_weight is not None:
        lr.fit(X, y, sample_weight)
    else:
        lr.fit(X, y)
    print("training mean accuracy = %.2f" % lr.score(X, y))
    print("testing mean accuracy = %.2f" % lr.score(X_test_trans, y_test))
    if col_names is not None:
        c = np.column_stack((col_names, np.round(lr.coef_.flatten(),2)))
        sorted_c = c[c[:,1].argsort()]
        print(sorted_c[:10])
        print(sorted_c[-10:])
    y_prob = lr.predict_proba(X_test_trans)
    end = datetime.datetime.now()
    delta = end - start
    y_pred = y_prob[:, 1] > t
    y_pred = y_pred.astype('uint8')
    print('--- t = %.2f results:' % t)
    print_results(y_test, y_pred)
    print('total time predictions: %f (s)' % delta.total_seconds())
    print('time per query: %f (s)' % (delta.total_seconds() / len(y_pred)))
    false_preds = y_pred != y
    false_vectors = np.multiply(lr.coef_ * X[false_preds, :])
    c = np.column_stack((col_names, np.round(lr.coef_.flatten(),2)))
    print(vector.shape)
    return y_pred

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
    print("bad queries ratio: %.2f" % (100 * np.sum(labels) / labels.shape[0]))
    X, X_test, y, y_test = train_test_split(df, labels, stratify=labels,
                                            test_size=test_size, random_state=1)
    sample_weight = X['freq']
    X = X.drop(['query', 'freq', 'cache', 'full'], axis=1)
    test_queries = X_test['query']
    test_freq = X_test['freq']
    subset_mrr = X_test['cache']
    db_mrr = X_test['full']
    X_test = X_test.drop(['query', 'freq', 'cache', 'full'], axis=1)
    #print(df.corr()['label'].sort_values())
    print("train set size, bad queries and bad query ratio: %d, %d, %.2f"
          % (y.shape[0], np.sum(y), (100 * np.sum(y) / y.shape[0])))
    print("test set size, bad queries and bad query ratio: %d, %d, %.2f"
          % (y_test.shape[0], np.sum(y_test), (100 * np.sum(y_test) / y_test.shape[0])))
    # learn the model
    y_pred = train_lr(X, y, X_test, y_test, t, df.columns.values[2:-2])
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
