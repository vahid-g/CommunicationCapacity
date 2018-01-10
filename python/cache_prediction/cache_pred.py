# runs simple classification on cache selection problem
from __future__ import division

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn import linear_model
from sklearn.metrics import confusion_matrix

df = pd.read_csv('data.csv')
df = df.sample(frac=1) #shuffles rows
df_size = df.shape[0]
split_index = int(df_size/2)
df_train = df.iloc[:split_index, :]
df_test = df.iloc[split_index+1:, :]

# learn the model
df_train = df_train.drop(['query', 'mrr_1', 'mrr_100'], axis = 1)
X = df_train.iloc[:,:-1]
y = df_train.iloc[:,-1]
sc = StandardScaler().fit(X)
X = sc.transform(X)
lr = linear_model.LogisticRegression()
lr.fit(X, y)
print("training mean accuracy = %f" % lr.score(X, y))

df_tmp = df_test.drop(['query', 'mrr_1', 'mrr_100'], axis = 1)
X_test = df_tmp.iloc[:,:-1]
y_test = df_tmp.iloc[:,-1]
X_test = sc.transform(X_test)
print("testing mean accuracy = %f" % lr.score(X_test, y_test))

y_pred = lr.predict(X_test)
tn, fp, fn, tp = confusion_matrix(y_test, y_pred).ravel()
print("Specifity = %f" % (tn / (tn + fp)))
print("tn = %d" % tn)

print("balanced learning")
lr = linear_model.LogisticRegression(class_weight='balanced')
lr.fit(X, y)
print('coefs:')
print(lr.coef_)
X_test = df_tmp.iloc[:,:-1]
y_test = df_tmp.iloc[:,-1]
X_test = sc.transform(X_test)
print("testing mean accuracy = %f" % lr.score(X_test, y_test))

y_pred = lr.predict(X_test)
tn, fp, fn, tp = confusion_matrix(y_test, y_pred).ravel()
print("Specifity = %f" % (tn / (tn + fp)))
print("tn = %d" % tn)

df_test['pred'] = pd.Series(y_pred, index=df_test.index)
df_test.to_csv('result.csv')
