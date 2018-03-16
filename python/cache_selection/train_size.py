# runs simple classification on cache selection problem
from __future__ import division
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.model_selection import GridSearchCV
from sklearn.preprocessing import StandardScaler
from sklearn import linear_model
from sklearn.metrics import confusion_matrix
from sklearn import svm
from sklearn.ensemble import RandomForestClassifier
from cs_helper import print_results

#df = pd.read_csv('../../data/python_data/cache_pred_new.csv')
df = pd.read_csv('../../data/python_data/cs_data_1.csv')
df = df.fillna(0)
df = df.drop(['Bm_25', 'min_bm25', 'bool_score', 'min_bool_score', 'Bm_25_sub',
'min_bm25_sub', 'bool_score_sub', 'min_bool_score_sub'], axis=1)
df_size = df.shape[0]
def train_lr(df, size):
    train = df.sample(frac=size, random_state=1)
    train = train.drop(['query'], axis=1)
    test = df.loc[~df.index.isin(train.index)]
    test_queries = test['query']
    test = test.drop(['query'], axis=1)
    cols = train.columns.tolist()
    # learn the model
    X = train[cols[:-1]]
    y = train[cols[-1]]
    print("1s percentage in training = %.2f" % (100 * np.sum(y) / y.shape[0]))
    sc = StandardScaler().fit(X)
    X = sc.transform(X)
    lr = linear_model.LogisticRegression(class_weight='balanced')
    lr.fit(X, y)
    print("training mean accuracy = %.2f" % lr.score(X, y))
    X_test = test[cols[:-1]]
    y_test = test[cols[-1]]
    X_test = sc.transform(X_test)
    print("testing mean accuracy = %.2f" % lr.score(X_test, y_test))
    print("threshold learning 0.8")
    y_prob = lr.predict_proba(X_test)
    y_pred = y_prob[:, 1] > 0.8
    y_pred = y_pred.astype('uint8')
    print_results(y_test, y_pred)

for s in np.arange(0.1, 0.9, 0.1):
    print("s = %.2f" % s)
    train_lr(df, s)
