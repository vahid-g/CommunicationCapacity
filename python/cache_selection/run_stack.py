''' runs simple classification on cache selection problem '''
from __future__ import division
import sys
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from sklearn import linear_model
from utils import print_results
import datetime

def f(row):
    if row['Pred'] == 1:
        val = row['100']
    else:
        val = row['18']
    return val
def g(row):
    if row['Label'] == 1:
        val = row['100']
    else:
        val = row['18']
    return val


def main(argv):
    filename = argv[0]
    t = float(argv[1])
    size = 0.33
    df = pd.read_csv('../../data/python_data/' + filename)
    labels = df['Label']
    df = df.drop(['Id', 'Label'], axis=1)
    #print(df.corr()['Label'].sort_values())
    X, X_test, y, y_test = train_test_split(df, labels, stratify=labels,
                                            test_size=size, random_state=1)
    X = X.drop(['TestViewCount', 'Query', '18', '100'], axis=1)
    vc = X_test['TestViewCount']
    test_queries = X_test['Query']
    q18 = X_test['18']
    q100 = X_test['100']
    X_test = X_test.drop(['TestViewCount', 'Query', '18', '100'], axis=1)
    ql = q18.copy()
    ql_pred = X_test['ql_t'] < X_test['ql_t.1']
    ql.loc[ql_pred == 1] = q100[ql_pred == 1]
    print("train set size and ones: %d, %d" % (y.shape[0], np.sum(y)))
    print("test set size and ones: %d, %d" % (y_test.shape[0], np.sum(y_test)))
    print("onez ratio in trian set =  %.2f" % (100 * np.sum(y) / y.shape[0]))
    print("onez ratio in test set =  %.2f" % (100 * np.sum(y_test) / y_test.shape[0]))
    # learn the model
    #sc = StandardScaler().fit(X)
    sc = MinMaxScaler().fit(X)
    X = sc.transform(X)
    X_test = sc.transform(X_test)
    print("training LR..")
    lr = linear_model.LogisticRegression()
    lr.fit(X, y)
    # lr.fit(X, y, sample_weight=vcx)
    print("train/test mean accuracy = %.2f, %.2f" %
          (lr.score(X, y), lr.score(X_test, y_test)))
    y_pred = lr.predict(X_test)
    print_results(y_test, y_pred)
    print("training balanced LR..")
    lr = linear_model.LogisticRegression(class_weight='balanced')
    lr.fit(X, y)
    #lr.fit(X, y, sample_weight=vcx)
    print("train/test mean accuracy = %.2f, %.2f" %
          (lr.score(X, y), lr.score(X_test, y_test)))
    #c = np.column_stack((df.columns.values[1:-1], np.round(lr.coef_.flatten(),2)))
    #print(c[c[:,1].argsort()])
    start = datetime.datetime.now()
    y_prob = lr.predict_proba(X_test)
    y_pred = y_prob[:, 1] > t
    y_pred = y_pred.astype('uint8')
    print(y_pred.shape)
    end = datetime.datetime.now()
    print('--- results:')
    print_results(y_test, y_pred)
    delta = end - start
    print('total time: %f' % delta.total_seconds())
    print('time per query: %f' % (delta.total_seconds() / len(y_pred)))
    print('test size (distinct): % d' % y_pred.size)
    print('test size (all): % d' % vc.sum())
    ones = vc * y_pred
    print('ones ratio: %.2f' % (ones.sum() / y_pred.size))
    output = pd.DataFrame()
    output['Query'] = test_queries
    output['TestViewCount'] = vc
    output['Label'] = y_test
    output['Pred'] = pd.Series(y_pred, index=output.index)
    output['18'] = q18
    output['100'] = q100
    output['ql'] = ql
    output['ml'] = output.apply(f, axis=1)
    output['best'] = output.apply(g, axis=1)
    if (argv[2]):
        output.to_csv('../../data/python_data/%s_result.csv' %
                  filename[:-4], index=False)

if __name__ == "__main__":
    main(sys.argv[1:])
