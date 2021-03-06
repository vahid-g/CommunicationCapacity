# runs simple classification on cache selection problem
from __future__ import division
import sys
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.model_selection import GridSearchCV
from sklearn.preprocessing import StandardScaler
from sklearn import linear_model
from sklearn.metrics import precision_score
from sklearn import svm
from sklearn.ensemble import RandomForestClassifier
from cs_helper import print_results

def main(argv):
    # argv = '../../data/python_data/cs_data_2.csv'
    df = pd.read_csv(argv)
    df = df.fillna(0)
    df_size = df.shape[0]
    train = df.sample(frac=0.66, random_state=1)
    train = train.drop(['query'], axis=1)
    test = df.loc[~df.index.isin(train.index)]
    test_queries = test['query']
    test = test.drop(['query'], axis=1)
    cols = train.columns.tolist()
    # print(df.corr()['label'].sort_values())

    X = train[cols[:-1]]
    y = train[cols[-1]]
    print("1s percentage in training = %.2f" % (100 * np.sum(y) / y.shape[0]))

    # learn the model
    print("learning logistic regression..")
    sc = StandardScaler().fit(X)
    X = sc.transform(X)
    lr = linear_model.LogisticRegression()
    lr.fit(X, y)
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
    X_test = test[cols[:-1]]
    y_test = test[cols[-1]]
    X_test = sc.transform(X_test)
    print("training mean accuracy = %.2f" % lr.score(X, y))
    print("testing mean accuracy = %.2f" % lr.score(X_test, y_test))
    y_pred = lr.predict(X_test)
    print_results(y_test, y_pred)

    print("balanced learning")
    lr = linear_model.LogisticRegression(class_weight='balanced')
    lr.fit(X, y)
    print("training mean accuracy = %.2f" % lr.score(X, y))
    print("testing mean accuracy = %.2f" % lr.score(X_test, y_test))
    # print('coefs:')
    # coef = np.sort(lr.coef_.flatten())
    # print(np.column_stack((train.columns.values[:-1], coef)))
    y_pred = lr.predict(X_test)
    y_prob = lr.predict_proba(X_test)
    y_pred = y_prob[:, 1] > 0.5
    print("LR with threshold 0.5")
    print_results(y_test, y_pred)
    print_results(y_test, y_pred)
    print("LR with threshold 0.8")
    y_pred = y_prob[:, 1] > 0.8
    y_pred = y_pred.astype('uint8')
    print_results(y_test, y_pred)

    '''
    print("random forest..")
    clf = RandomForestClassifier(n_estimators=10, class_weight='balanced')
    clf.fit(X,y)
    print("training mean accuracy = %.2f" % lr.score(X, y))
    y_pred = clf.predict(X_test)
    print_results(y_test, y_pred)

    print("random forest with n = 50 ")
    clf = RandomForestClassifier(n_estimators=50, class_weight='balanced')
    clf.fit(X,y)
    print("training mean accuracy = %.2f" % lr.score(X, y))
    y_pred = clf.predict(X_test)
    print_results(y_test, y_pred)

    print("unbalanced random forest..")
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
    output.to_csv('../../data/python_data/cs_result_2.csv')
    '''

if __name__ == "__main__":
    main(sys.argv[1])
