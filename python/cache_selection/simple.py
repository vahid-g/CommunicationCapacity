# runs simple classification on cache selection problem
from __future__ import division

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn import linear_model

df = pd.read_csv('Desktop/data.csv')
df = df.drop(['query', 'mrr_1', 'mrr_100'], axis = 1)
X = df.iloc[:,:-1]
y = df.iloc[:,-1]
X = StandardScaler().fit(X).transform(X)
X_train, X_test, y_train, y_test = train_test_split(X, y,
        random_state=42,
        stratify=y,
        test_size=0.5)
lr = linear_model.LogisticRegression(C=1e5)
lr.fit(X_train, y_train)
y_pred = lr.predict(X_test)
print("mean accuracy = %f" % lr.score(X_test, y_test))

