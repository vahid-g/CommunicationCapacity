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

def main(argv):
    filename = argv[0]
    df = pd.read_csv('../../data/cache_selection_structured/' + filename)
    print('df size: ' + str(df.shape))
    t = float(argv[1])
    df = df.fillna(0)
    labels = np.where(df['full'] > df['cache'], 1, 0)
    print("onez ratio: %.2f" % (100 * np.sum(labels) / labels.shape[0]))
    size = 0.33
    X, X_test, y, y_test = train_test_split(df, labels, stratify=labels,
                                            test_size=size, random_state=1)
    X = X.drop(['query', 'freq', 'cache', 'full'], axis=1)
    test_queries = X_test['query']
    test_freq = X_test['freq']
    subset_mrr = X_test['cache']
    db_mrr = X_test['full']
    X_test = X_test.drop(['query', 'freq', 'cache', 'full'], axis=1)
    ql = subset_mrr.copy()
    ql_pred = X_test['ql'] < X_test['ql_rest']
    ql.loc[ql_pred == 1] = db_mrr[ql_pred == 1]
    #print(df.corr()['label'].sort_values())
    print("train set size and ones: %d, %d" % (y.shape[0], np.sum(y)))
    print("test set size and ones: %d, %d" % (y_test.shape[0], np.sum(y_test)))
    print("onez ratio in trian set =  %.2f" % (100 * np.sum(y) / y.shape[0]))
    print("onez ratio in test set =  %.2f" % (100 * np.sum(y_test) / y_test.shape[0]))
    # learn the model
    #sc = StandardScaler().fit(X)
    sc = MinMaxScaler().fit(X)
    X = sc.transform(X)
    X_test = sc.transform(X_test)
    print("training balanced LR..")
    lr = linear_model.LogisticRegression(class_weight='balanced')
    lr.fit(X, y)
    print("training mean accuracy = %.2f" % lr.score(X, y))
    print("testing mean accuracy = %.2f" % lr.score(X_test, y_test))
    #c = np.column_stack((df.columns.values[5:-1], np.round(lr.coef_.flatten(),2)))
    #print(c[c[:,1].argsort()])
    y_prob = lr.predict_proba(X_test)
    y_pred = y_prob[:, 1] > t
    y_pred = y_pred.astype('uint8')
    print('--- t = %.2f results:' % t)
    print_results(y_test, y_pred)
    output = pd.DataFrame()
    output['Query'] = test_queries
    output['TestFreq'] = test_freq
    output['cache'] = subset_mrr
    output['full'] = db_mrr
    output['Label'] = y_test
    output['ql'] = ql
    #output['ql_label'] = ql
    ml = subset_mrr.copy()
    ml.loc[y_pred == 1] = db_mrr[y_pred == 1]
    output['ml'] = ml
    #output['Pred'] = pd.Series(y_pred, index=output.index)
    best = subset_mrr.copy()
    print(best.mean())
    best[y_test == 1] = db_mrr[y_test == 1]
    print(best.mean())
    output['best'] = best
    r = np.random.randint(0, 2, output['cache'].size)
    output['rand'] = np.where(r == 1, output['full'], output['cache'])
    # output['rand'] = output['cache'].copy()
    # output['rand'][r == 1] = output['full'][r == 1].copy()
    analyze(output, 'cache', 'full','TestFreq')
    if (argv[2]):
        output.to_csv('%s%s_result.csv' % ('../../data/cache_selection_structured/',
                                       filename[:-4]), index=False)

if __name__ == "__main__":
    main(sys.argv[1:])
