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

def print_results(y_test, y_pred):
    tn, fp, fn, tp = confusion_matrix(y_test, y_pred).ravel()
    print("precision = %f" % (tp / (tp + fp)))
    print("recall = %.2f" % (tp / (tp + fn)))
    print("negative predictive value= %.2f" % (tn / (tn + fn)))
    print("true negative rate= %.2f" % (tn / (tn + fp)))
    print("1s percentage = %.2f" % (100 * np.sum(y_pred) / y_pred.shape[0]))

def train_lr(df, size=0.33):
    df = df.fillna(0)
    '''
    df['diff_covered_bi_c'] = df['covered_bi_c_rest'] - df['covered_bi_c']
    df['diff_covered_term_t'] = df['covered_term_t_rest'] - df['covered_term_t']
    df['diff_covered_term_c'] = df['covered_term_c_rest'] - df['covered_term_c']
    df = df.drop(['covered_bi_c_rest', 'covered_bi_c', 'covered_bi_c_full',
                  'covered_term_t_rest', 'covered_term_t',
                  'covered_term_t_full', 'covered_term_c',
                  'covered_term_c_rest', 'covered_term_c_full'], axis=1)
    '''
    labels = df['label']
    X, X_test, y, y_test = train_test_split(df.drop(['label'], axis=1), labels, stratify=labels,
                                            test_size=size, random_state=1)
    X = X.drop(['query'], axis=1)
    test_queries = X_test['query']
    X_test = X_test.drop(['query'], axis=1)
    #print(df.corr()['label'].sort_values())
    print("train set size and ones: %d, %d" % (y.shape[0], np.sum(y)))
    print("test set size and ones: %d, %d" % (y_test.shape[0], np.sum(y_test)))
    print("onez ratio in trian set =  %.2f" % (100 * np.sum(y) / y.shape[0]))
    print("onez ratio in test set =  %.2f" % (100 * np.sum(y_test) / y_test.shape[0]))
    # learn the model
    # print("logistic regression..")
    #sc = StandardScaler().fit(X)
    sc = MinMaxScaler().fit(X)
    X = sc.transform(X)
    ''' parameter tuning
    tuned_parameters = [{'C': [0.01, 0.1, 1, 10, 100, 1000]}]
    scores = ['precision', 'recall']
    for score in scores:
    clf = GridSearchCV(linear_model.LogisticRegression(), tuned_parameters, cv=5,
    scoring='%s_macro' % score)
    clf.fit(X, y)
    print('best params:')
    print(clf.best_params_)
    '''
    X_test = sc.transform(X_test)
    print("balanced logistic regression threshold=0.8 ..")
    lr = linear_model.LogisticRegression(class_weight='balanced')
    lr.fit(X, y)
    print("training mean accuracy = %.2f" % lr.score(X, y))
    print("testing mean accuracy = %.2f" % lr.score(X_test, y_test))
    c = np.column_stack((df.columns.values[1:-1], lr.coef_.flatten()))
    print(c[c[:,1].argsort()])
    y_prob = lr.predict_proba(X_test)
    y_pred = y_prob[:, 1] > 0.75
    y_pred = y_pred.astype('uint8')
    print_results(y_test, y_pred)
    '''
    x = np.round(X_test[12,:], 2)
    c = np.round(lr.coef_.flatten(), 2)
    x = np.column_stack((df.columns.values[1:-1], c, x, np.round(x * c, 2)))
    print('%s %.2f ' % (test_queries.iloc[12], y_prob[12,1]))
    print(x[x[:,3].argsort()])
    '''
    '''
    #grid search for logistic regression threshold
    for t in np.arange(0.5, 1, 0.1):
        print("threshold = %.2f" % t)
        y_pred = y_prob[:, 1] > t
        tn, fp, fn, tp = confusion_matrix(y_test, y_pred).ravel()
        print_results(y_test, y_pred)

    '''
    output = pd.DataFrame()
    output['query'] = test_queries
    output['label'] = y_test
    output['pred'] = pd.Series(y_pred, index=output.index)
    return output
