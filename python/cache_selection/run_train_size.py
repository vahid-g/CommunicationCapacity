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
from sklearn.metrics import confusion_matrix

def main(argv):
    filename = argv
    df = pd.read_csv('../../data/python_data/' + filename)
    for s in np.arange(0.1, 0.9, 0.1):
        print("s = %.2f" % s)
        train_lr(df, s)

def train_lr(df, size):
    df = df.fillna(0)
    cols = df.columns.tolist()
    labels = df[cols[-1]]
    X, X_test, y, y_test = train_test_split(df[cols[:-1]], labels, stratify=labels,
                                            test_size=size, random_state=1)
    X = X.drop(['query'], axis=1)
    test_queries = X_test['query']
    X_test = X_test.drop(['query'], axis=1)
    # print("train set size and ones: %d, %d" % (y.shape[0], np.sum(y)))
    # print("test set size and ones: %d, %d" % (y_test.shape[0], np.sum(y_test)))
    # print("onez ratio in trian set =  %.2f" % (100 * np.sum(y) / y.shape[0]))
    # print("onez ratio in test set =  %.2f" % (100 * np.sum(y_test) / y_test.shape[0]))
    sc = StandardScaler().fit(X)
    X = sc.transform(X)
    X_test = sc.transform(X_test)
    lr = linear_model.LogisticRegression(class_weight='balanced')
    lr.fit(X, y)
    # print("training mean accuracy = %.2f" % lr.score(X, y))
    # print("testing mean accuracy = %.2f" % lr.score(X_test, y_test))
    y_pred = lr.predict(X_test)
    y_prob = lr.predict_proba(X_test)
    y_8 = y_prob[:, 1] > 0.8
    tn, fp, fn, tp = confusion_matrix(y_test, y_pred).ravel()
    print("pre = %.2f fallout = %.2f" % (tp / (tp + fp), (tn / (tn + fp))))


if __name__ == "__main__":
    main(sys.argv[1])
