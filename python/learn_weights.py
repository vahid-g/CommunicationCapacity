import numpy as np
import pandas as pd 
from sklearn import linear_model
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import cross_val_score
from sklearn.metrics import mean_squared_error, r2_score
import matplotlib.pyplot as plt
import math
from sklearn.naive_bayes import GaussianNB
from sklearn import preprocessing

df = pd.read_csv('../data/amazon_data/trainset.csv', header=None, low_memory=False)
print('initial shape: ', df.shape)
print(df.iloc[0,:])
isbns = df.as_matrix([0])
del(df[0])
df[1] = df[1].astype('category')
cat_cols = df.select_dtypes(['category']).columns
df[cat_cols] = df[cat_cols].apply(lambda x: x.cat.codes)
df[4] = df[4].str.split("[.']").str.get(0).astype('float')
df = df.fillna(df.mean())
print('after prep: ',  df.shape)
print(df.iloc[0, :])
t_size = int(df.shape[0] * 0.75)
X = df.iloc[:t_size,:-1]
scaler = preprocessing.StandardScaler().fit(X)
X = scaler.transform(X)
X_test = scaler.transform(df.iloc[t_size:,:-1])
y = df.iloc[:t_size,-1:]
y_test = df.iloc[t_size:,-1:]
isb = isbns[:t_size]
isb_test = isbns[t_size:]
regr = linear_model.LinearRegression()
#regr = GaussianNB()
#regr = RandomForestClassifier()
regr.fit(X, np.ravel(y))
#res = cross_val_score(regr, X, np.ravel(y), cv=10, scoring='r2')
#print(np.mean(res))
#print(regr.score(X,y))
#print(regr.score(X_test,y_test))
pred = regr.predict(X_test)
pred =np.reshape(pred, (len(pred), 1))
final = np.concatenate((isb_test, pred), 1)
print("mean squared: %.2f" % mean_squared_error(y_test, pred))
print("root mean squared: %.2f" % np.sqrt(mean_squared_error(y_test, pred)))
print('Variance: %.2f' % r2_score(y_test, pred))
np.savetxt('../data/amazon_data/pred0.csv', final, fmt='%s, %.18e', delimiter=',')
'''
df = pd.read_csv('../data/amazon_data/dataset.csv', header=None, low_memory=False)
df[4] = df[4].str.split("[.']").str.get(0)
df[4] = df[4].astype('float')
print(isbns[0])
df[0] = df[0].astype('category')
df[1] = df[1].astype('category')
cat_cols = df.select_dtypes(['category']).columns
df[cat_cols] = df[cat_cols].apply(lambda x: x.cat.codes)
df = df.fillna(df.mean())
del(df[0])
print(df.shape)
pred = regr.predict(df)
pred =np.reshape(pred, (len(pred), 1))
print(pred.shape)
print(isbns.shape)
final = np.concatenate((isbns, pred), 1)
print(final.shape)
print(final[0])
np.savetxt('../data/amazon_data/pred.csv', final, fmt='%s, %.18e', delimiter=',')
'''
