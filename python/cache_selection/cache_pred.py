''' runs simple classification on cache selection problem '''
from __future__ import division
import sys
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
# from sklearn.model_selection import GridSearchCV
# from sklearn.model_selection import StratifiedShuffleSplit
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import RobustScaler
from sklearn.preprocessing import MinMaxScaler
from sklearn import linear_model
# from sklearn.metrics import precision_score
# from sklearn import svm
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import confusion_matrix

def train_lr(df, size=0.33):
    df = df.fillna(0)
    labels = df['label']
    X, X_test, y, y_test = train_test_split(df.drop(['label'], axis=1), labels, stratify=labels,
                                            test_size=size, random_state=1)
    # df = df.drop(['viewcount'], axis=1)
    # vcx = X['viewcount']
    # X = X.drop(['viewcount'], axis=1)
    X = X.drop(['query'], axis=1)
    test_queries = X_test['query']
    #vcy = X_test['viewcount']
    #X_test = X_test.drop(['viewcount'], axis=1)
    X_test = X_test.drop(['query'], axis=1)
    print(df.corr()['label'].sort_values())
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
    y_prob = lr.predict_proba(X_test)
    y_pred = y_prob[:, 1] > 0.5
    y_pred = y_pred.astype('uint8')
    '''
    print('--- t = 0.5 results:')
    print_results(y_test, y_pred)
    y_pred = y_prob[:, 1] > 0.75
    y_pred = y_pred.astype('uint8')
    print('--- t = 0.75 results:')
    print_results(y_test, y_pred)
    y_pred = y_prob[:, 1] > 0.8
    y_pred = y_pred.astype('uint8')
    print('--- t = 0.8 results:')
    print_results(y_test, y_pred)
    '''
    output = pd.DataFrame()
    output['query'] = test_queries
    output['label'] = y_test
    output['pred'] = pd.Series(y_pred, index=output.index)
    return output

def train_wiki(df, size=0.33):
    df = df.fillna(0)
    labels = df['label']
    X, X_test, y, y_test = train_test_split(df.drop(['label'], axis=1), labels, stratify=labels,
                                            test_size=size, random_state=1)
    X = X.drop(['query'], axis=1)
    test_queries = X_test['query']
    X_test = X_test.drop(['query'], axis=1)
    print(df.corr()['label'].sort_values())
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
    c = np.column_stack((df.columns.values[1:-1], np.round(lr.coef_.flatten(),
                                                          2)))
    print(c[c[:,1].argsort()])
    y_prob = lr.predict_proba(X_test)
    y_pred = y_prob[:, 1] > 0.5
    y_pred = y_pred.astype('uint8')
    print('--- t = 0.5 results:')
    print_results(y_test, y_pred)
    y_pred = y_prob[:, 1] > 0.75
    y_pred = y_pred.astype('uint8')
    print('--- t = 0.75 results:')
    print_results(y_test, y_pred)
    y_pred = y_prob[:, 1] > 0.8
    y_pred = y_pred.astype('uint8')
    print('--- t = 0.8 results:')
    print_results(y_test, y_pred)
    '''
    x = np.round(X_test[12,:], 2)
    c = np.round(lr.coef_.flatten(), 2)
    x = np.column_stack((df.columns.values[1:-1], c, x, np.round(x * c, 2)))
    print('%s %.2f ' % (test_queries.iloc[12], y_prob[12,1]))
    print(x[x[:,3].argsort()])
    '''
    output = pd.DataFrame()
    output['query'] = test_queries
    output['label'] = y_test
    output['pred'] = pd.Series(y_pred, index=output.index)
    return output


def print_results(y_test, y_pred):
    tn, fp, fn, tp = confusion_matrix(y_test, y_pred).ravel()
    print("  precision = %f" % (tp / (tp + fp)))
    print("  recall = %.2f" % (tp / (tp + fn)))
    print("  negative predictive value= %.2f" % (tn / (tn + fn)))
    print("  true negative rate= %.2f" % (tn / (tn + fp)))
    print("  1s percentage = %.2f" % (100 * np.sum(y_pred) / y_pred.shape[0]))

def param_tuning(X, y):
    tuned_parameters = [{'C': [0.01, 0.1, 1, 10, 100, 1000]}]
    scores = ['precision', 'recall']
    for score in scores:
        clf = GridSearchCV(linear_model.LogisticRegression(), tuned_parameters, cv=5,
                           scoring='%s_macro' % score)
        clf.fit(X, y)
        print('best params:')
        print(clf.best_params_)

def grid_search(y_prob, y_test):
    # grid search for logistic regression threshold
    for t in np.arange(0.5, 1, 0.1):
        print("threshold = %.2f" % t)
        y_pred = y_prob[:, 1] > t
        tn, fp, fn, tp = confusion_matrix(y_test, y_pred).ravel()
        print_results(y_test, y_pred)
