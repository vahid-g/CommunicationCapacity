''' runs simple classification on cache selection problem '''
from __future__ import division
import sys
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import MinMaxScaler
from sklearn import linear_model
from sklearn.model_selection import StratifiedKFold
from sklearn.model_selection import LeaveOneOut
from sklearn.model_selection import RepeatedStratifiedKFold
from utils import print_results

def main(argv):
    filename = argv[0]
    t = float(argv[1]) # threshold for logistic regression (default=0.5)
    dup = int(argv[2]) # if 1, bad queries will be duplicated
    subset = 'cache' # column title for precision of cache
    full = 'full' # column title for precision of full db
    df = pd.read_csv('../../data/cache_selection_structured/' + filename)
    df = df.drop(['query', 'freq'], axis = 1)
    df = df.fillna(0)
    df['label'] = np.where(df['full'] > df['cache'], 1, 0)
    if dup:
        print('duping..')
        bads = df[df['label'] == 1]
        df = df.append(bads, ignore_index=True)
    X = df.drop(['label'], axis = 1)
    y = df['label']
    p20_mean = np.zeros([1, 6])
    bad_mean = np.zeros([1, 6])
    ml_average_rare = 0
    ql_average_rare = 0
    best_average_rare = 0
    loo = LeaveOneOut()
    bad_counter = 0
    for train_index, test_index in loo.split(X):
        X_train, X_test = X.iloc[train_index], X.iloc[test_index]
        y_train, y_test = y.iloc[train_index], y.iloc[test_index]
        X_train = X_train.drop([subset, full], axis=1)
        p12 = X_test[subset].iloc[0]
        p100 = X_test[full].iloc[0]
        is_bad = p12 < p100
        X_test = X_test.drop([subset, full], axis=1)
        # compute query likelihood based effectiveness
        ql_cache = np.mean(X_test['ql_0_0'] + X_test['ql_0_1'] +
                           X_test['ql_1_0'] + X_test['ql_2_0'])
        ql_rest = np.mean(X_test['ql_rest_0_0'] + X_test['ql_rest_0_1'] +
                           X_test['ql_rest_1_0'] + X_test['ql_rest_2_0'])
        #ql_pred = X_test['ql_0_1'].iloc[0] < X_test['ql_rest_0_1'].iloc[0]
        ql_pred = 1 if ql_cache < ql_rest else 0
        ql = p12 if ql_pred == 0 else p100
        # learn the model
        sc = MinMaxScaler().fit(X_train)
        X_train = sc.transform(X_train)
        X_test = sc.transform(X_test)
        # print("\ttraining balanced LR..")
        lr = linear_model.LogisticRegression(class_weight='balanced')
        lr.fit(X_train, y_train)
        # print("\ttraining mean accuracy = %.2f" % lr.score(X_train, y_train))
        # print("\ttesting mean accuracy = %.2f" % lr.score(X_test, y_test))
        y_prob = lr.predict_proba(X_test)
        y_pred = y_prob[:, 1] > t
        y_pred = y_pred.astype('uint8')
        # print('\t t = %.2f results:' % t)
        # print_results(y_test, y_pred)
        # compute ML based effectiveness
        ml = p12 if y_pred[0] == 0 else p100
        best = p12 if y_test.iloc[0] == 0 else p100
        rnd = p12 if np.random.randint(0, 2) == 1 else p100
        p20_mean += [p12, p100, ml, ql,
                     best, rnd]
        if is_bad:
            #bad_mean += [p12[0], p100[0], ml[0], ql[0], best[0], rnd[0]]
            bad_mean += [p12, p100, ml, ql, best, rnd]
            bad_counter += 1
    print('final results:')
    print('\t'.join(map(str,['set', 'cache', 'db', 'ml', 'ql', 'best',
                              'rand'])))
    print('\t'.join(['bad'] + map(str, np.round(bad_mean[0] / bad_counter, 2))))
    print('\t'.join(['all'] + map(str, np.round(p20_mean[0] / df.shape[0], 2))))

if __name__ == "__main__":
    main(sys.argv[1:])
