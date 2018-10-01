from __future__ import division
import sys
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import RobustScaler
from sklearn.preprocessing import MinMaxScaler
from sklearn import linear_model
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import confusion_matrix
import datetime


def print_results(y_test, y_pred):
    if (y_test.size > 1):
        tn, fp, fn, tp = confusion_matrix(y_test, y_pred).ravel()
        print("\t  precision = %f" % (tp / (tp + fp)))
        print("\t  recall = %.2f" % (tp / (tp + fn)))
        print("\t  negative predictive value= %.2f" % (tn / (tn + fn)))
        print("\t  true negative rate= %.2f" % (tn / (tn + fp)))
        print("\t  1s percentage = %.2f" % (100 * np.sum(y_pred) / y_pred.shape[0]))
    else:
        print(" can not compute confusion matrix when |y_test| = 1")

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
