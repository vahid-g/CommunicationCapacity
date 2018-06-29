''' runs simple classification on cache selection problem '''
from __future__ import division
import sys
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import MinMaxScaler
from sklearn import linear_model
from sklearn.model_selection import StratifiedKFold
from sklearn.model_selection import RepeatedStratifiedKFold
from utils import print_results

def main(argv):
    filename = argv[0]
    t = float(argv[1])
    subset = argv[2]
    full = '100'
    split = 5
    df = pd.read_csv('../../data/python_data/' + filename)
    df = df.drop(['query'], axis = 1)
    print('bad queries ratio = %.2f' % (df['label'].sum() / df['label'].size))
    skf = StratifiedKFold(n_splits=split, random_state = 1)
    X = df.drop(['label'], axis = 1)
    y = df['label']
    p20_mean = np.zeros([1, 5])
    bad_mean = np.zeros([1, 5])
    ml_average_rare = 0
    ql_average_rare = 0
    best_average_rare = 0
    for train_index, test_index in skf.split(X, y):
        X_train, X_test = X.iloc[train_index], X.iloc[test_index]
        y_train, y_test = y.iloc[train_index], y.iloc[test_index]
        X_train = X_train.drop([subset, full], axis=1)
        p12 = X_test[subset]
        p100 = X_test[full]
        bad_index = p100 > p12
        X_test = X_test.drop([subset, full], axis=1)
        # compute query likelihood based effectiveness
        ql_pred = X_test['ql_c'] < X_test['ql_c.1']
        ql = p12.copy()
        ql.loc[ql_pred == 1] = p100[ql_pred == 1]
        ql_average_rare += (ql_pred.sum() / ql_pred.size)
        print("\ttrain set size and ones: %d, %d" % (y_train.shape[0], np.sum(y_train)))
        print("\ttest set size and ones: %d, %d" % (y_test.shape[0], np.sum(y_test)))
        print("\tonez ratio in trian set =  %.2f" % (100 * np.sum(y_train) / y_train.shape[0]))
        print("\tonez ratio in test set =  %.2f" % (100 * np.sum(y_test) / y_test.shape[0]))
        # learn the model
        sc = MinMaxScaler().fit(X_train)
        X_train = sc.transform(X_train)
        X_test = sc.transform(X_test)
        print("\ttraining balanced LR..")
        lr = linear_model.LogisticRegression(class_weight='balanced')
        lr.fit(X_train, y_train)
        print("\ttraining mean accuracy = %.2f" % lr.score(X_train, y_train))
        print("\ttesting mean accuracy = %.2f" % lr.score(X_test, y_test))
        y_prob = lr.predict_proba(X_test)
        y_pred = y_prob[:, 1] > t
        y_pred = y_pred.astype('uint8')
        print('\t t = %.2f results:' % t)
        print_results(y_test, y_pred)
        # compute ML based effectiveness
        ml = p12.copy()
        ml.loc[y_pred == 1] = p100[y_pred == 1]
        best = p12.copy()
        best.loc[y_test == 1] = p100[y_test == 1]
        print('\t---')
        print('\tsubset mean p@20 = %.2f %.2f' % (p12.mean(),
                                                  p12[bad_index].mean()))
        print('\tdb mean p@20 = %.2f %.2f' % (p100.mean(),
                                                  p100[bad_index].mean()))
        print('\tml mean p@20 = %.2f %.2f' % (ml.mean(), ml[bad_index].mean()))
        print('\tql mean p@20 = %.2f %.2f' % (ql.mean(), ql[bad_index].mean()))
        print('\tbest mean p@20 = %.2f %.2f' %
              (best.mean(),best[bad_index].mean()))
        p20_mean += [p12.mean(), p100.mean(), ml.mean(), ql.mean(), best.mean()]
        bad_mean += [p12[bad_index].mean(), p100[bad_index].mean(),
                     ml[bad_index].mean(), ql[bad_index].mean(),
                    best[bad_index].mean()]
        ml_average_rare += (y_pred.sum() / y_pred.size)
    print('final results:')
    print([['subset', 'database', 'ml', 'ql', 'best']])
    print(bad_mean / split)
    print(p20_mean / split)
    print('average rare query count ml: %.2f ql: %.2f' %
          (ml_average_rare /split, ql_average_rare / split))

if __name__ == "__main__":
    main(sys.argv[1:])
