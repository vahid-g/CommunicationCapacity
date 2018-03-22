''' runs simple classification on cache selection problem '''
from __future__ import division
import sys
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
# from sklearn.model_selection import GridSearchCV
# from sklearn.model_selection import StratifiedShuffleSplit
from sklearn.preprocessing import StandardScaler
from sklearn import linear_model
# from sklearn.metrics import precision_score
# from sklearn import svm
from sklearn.ensemble import RandomForestClassifier
from cs_helper import print_results

def main(argv):
    filename = argv
    df = pd.read_csv('../../data/python_data/' + filename)
    df = df.fillna(0)
    cols = df.columns.tolist()
    labels = df[cols[-1]]
    X, X_test, y, y_test = train_test_split(df[cols[:-1]], labels, stratify=labels,
                                            test_size=0.3, random_state=1)
    X = X.drop(['query'], axis=1)
    test_queries = X_test['query']
    X_test = X_test.drop(['query'], axis=1)
    #print(df.corr()['label'].sort_values())

    print("train set size and ones: %d, %d" % (y.shape[0], np.sum(y)))
    print("test set size and ones: %d, %d" % (y_test.shape[0], np.sum(y_test)))
    print("onez ratio in trian set =  %.2f" % (100 * np.sum(y) / y.shape[0]))
    print("onez ratio in test set =  %.2f" % (100 * np.sum(y_test) / y_test.shape[0]))
    # learn the model
    print("\nlearning logistic regression..")
    sc = StandardScaler().fit(X)
    X = sc.transform(X)
    lr = linear_model.LogisticRegression()
    lr.fit(X, y)
    print("training mean accuracy = %.2f" % lr.score(X, y))
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
    print("testing mean accuracy = %.2f" % lr.score(X_test, y_test))
    y_pred = lr.predict(X_test)
    print_results(y_test, y_pred)

    print("\nbalanced learning")
    lr = linear_model.LogisticRegression(class_weight='balanced')
    lr.fit(X, y)
    print("training mean accuracy = %.2f" % lr.score(X, y))
    print("testing mean accuracy = %.2f" % lr.score(X_test, y_test))
    print('coefs:')
    coef = np.sort(lr.coef_.flatten())
    #print(np.column_stack((df.columns.values[1:-1], coef)))
    y_pred = lr.predict(X_test)
    y_prob = lr.predict_proba(X_test)
    y_5 = y_prob[:, 1] > 0.5
    print("\nLR with threshold 0.5")
    print_results(y_test, y_5)
    print("\nLR with threshold 0.8")
    y_8 = y_prob[:, 1] > 0.8
    # y_pred = y_pred.astype('uint8')
    print_results(y_test, y_8)

    '''
    print("\nbalanced random forest..")
    clf = RandomForestClassifier(n_estimators=10, class_weight='balanced')
    clf.fit(X,y)
    print("training mean accuracy = %.2f" % lr.score(X, y))
    y_pred = clf.predict(X_test)
    print_results(y_test, y_pred)

    print("\nbalanced random forest with n = 50 ")
    clf = RandomForestClassifier(n_estimators=50, class_weight='balanced')
    clf.fit(X,y)
    print("training mean accuracy = %.2f" % lr.score(X, y))
    y_pred = clf.predict(X_test)
    print_results(y_test, y_pred)

    print("\nunbalanced random forest..")
    clf = RandomForestClassifier(n_estimators=50)
    clf.fit(X,y)
    print("training mean accuracy = %.2f" % lr.score(X, y))
    y_pred = clf.predict(X_test)
    print_results(y_test, y_pred)
    print("svc results..")
    clf = svm.SVC(kernel='linear', class_weight='balanced')
    clf.fit(X, y)
    y_pred = clf.predict(X_test)
    print_results(y_test, y_pred)
    output = pd.DataFrame()
    output['query'] = test_queries
    output['pred'] = pd.Series(y_pred, index=output.index)
    output.to_csv('%s%s_result.csv' % ('../../data/python_data/', filename[:-4]))
    '''

if __name__ == "__main__":
    main(sys.argv[1])
