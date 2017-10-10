import math
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn import linear_model
from sklearn import preprocessing
from sklearn import tree
from sklearn.pipeline import Pipeline, make_pipeline
from sklearn.preprocessing import PolynomialFeatures
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import cross_val_score
from sklearn.metrics import mean_squared_error, r2_score, make_scorer
from sklearn.naive_bayes import GaussianNB
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.neural_network import MLPRegressor

df = pd.read_csv('../data/amazon/everything/features_labels.csv', header=None, low_memory=False)
print(df.describe())
print('data size: ' + repr(df.shape))
print('first row: \n' + repr(df.iloc[0, :]))
print('column types: \n' + repr(df.dtypes))

print('cleaning data..')
print('removing IDs..')
isbns = df.as_matrix([0])
del(df[0])
print('converting categorical values to numerical values')
df[1] = df[1].astype('category')
cat_cols = df.select_dtypes(['category']).columns
df[cat_cols] = df[cat_cols].apply(lambda x: x.cat.codes)
df[4] = df[4].str.split("[.']").str.get(0).astype('float')
df = df.fillna(df.mean())

print('data cleaning is done.')
print('data size' + repr(df.shape))
print('first row: \n' + repr(df.iloc[0, :]))

'''
print('preparing train/test data..')
train_size = int(df.shape[0] * 0.75)
X_train = df.iloc[:train_size,:-1]
scaler = preprocessing.StandardScaler().fit(X_train)
X = scaler.transform(df.iloc[:,:-1])
y = df.iloc[:, -1:]

X_test = scaler.transform(df.iloc[train_size:,:-1])
y_train = df.iloc[:train_size,-1:]
y_test = df.iloc[train_size:,-1:]
trian_ids = isbns[:train_size]
test_ids = isbns[train_size:]
'''

print('=== linear regression ===')
regr = linear_model.LinearRegression()
print('r2 = %.2f' % cross_val_score(regr, df.iloc[:, :-1], df.iloc[:,-1:], cv=10, scoring =
'r2').mean())
print('rmse = %.2f' % np.sqrt(-1 * cross_val_score(regr, df.iloc[:, :-1], df.iloc[:,-1:], cv=10, scoring =
'neg_mean_squared_error')).mean())
regr = Pipeline([('trans', preprocessing.StandardScaler()), ('regr', regr)])
print('r2 = %.2f' % cross_val_score(regr, df.iloc[:, :-1], df.iloc[:,-1:], cv=10, scoring =
'r2').mean())
print('rmse = %.2f' % np.sqrt(-1 * cross_val_score(regr, df.iloc[:, :-1], df.iloc[:,-1:], cv=10, scoring =
'neg_mean_squared_error')).mean())
regr = make_pipeline(preprocessing.StandardScaler(), linear_model.LinearRegression())
print('r2 = %.2f' % cross_val_score(regr, df.iloc[:, :-1], df.iloc[:,-1:], cv=10, scoring =
'r2').mean())
print('rmse = %.2f' % np.sqrt(-1 * cross_val_score(regr, df.iloc[:, :-1], df.iloc[:,-1:], cv=10, scoring =
'neg_mean_squared_error')).mean())

'''
print('=== ridge ===')
regr = linear_model.Ridge(alpha = .05)
print(cross_val_score(regr, df.iloc[:, :-1], df.iloc[:,-1:], cv = 10).mean())
print('=== lasso ===')
regr = linear_model.Lasso(alpha = .05)
print(cross_val_score(regr, df.iloc[:, :-1], df.iloc[:,-1:], cv = 10).mean())
print('=== Poly Linear ===')
regr = Pipeline([('poly', PolynomialFeatures(degree=2)), ('linear',
    linear_model.LinearRegression(fit_intercept=False))])
print(cross_val_score(regr, df.iloc[:, :-1], df.iloc[:,-1:], cv = 10).mean())
'''
print('=== Decision Tree ===')
regr = tree.DecisionTreeRegressor()
print('r2 = %.2f' % cross_val_score(regr, df.iloc[:, :-1], df.iloc[:,-1:], cv = 10, scoring =
    'r2').mean())
print('rmse = %.2f' % np.sqrt(-1 * cross_val_score(regr, df.iloc[:, :-1], df.iloc[:,-1:], cv = 10, scoring =
    'neg_mean_squared_error')).mean())
regr = Pipeline([('trans', preprocessing.StandardScaler()), ('regr', regr)])
print('r2 = %.2f' % cross_val_score(regr, df.iloc[:, :-1], df.iloc[:,-1:], cv = 10, scoring =
    'r2').mean())
print('rmse = %.2f' % np.sqrt(-1 * cross_val_score(regr, df.iloc[:, :-1], df.iloc[:,-1:], cv = 10, scoring =
    'neg_mean_squared_error')).mean())

print('=== Gradient Boosting ===')
regr = GradientBoostingRegressor(n_estimators=100, learning_rate=0.1,
        max_depth=1, random_state=0, loss='ls')
print('r2 = %.2f' % cross_val_score(regr, df.iloc[:, :-1],
    df.iloc[:,-1:], cv = 10, scoring =
    'r2').mean())
print('rmse = %.2f' % np.sqrt(-1 * cross_val_score(regr, df.iloc[:, :-1],
    df.iloc[:,-1:] , cv = 10, scoring =
    'neg_mean_squared_error')).mean())
regr = Pipeline([('trans', preprocessing.StandardScaler()), ('regr', regr)])
print('r2 = %.2f' % cross_val_score(regr, df.iloc[:, :-1], df.iloc[:,-1:], cv = 10, scoring =
    'r2').mean())
print('rmse = %.2f' % np.sqrt(-1 * cross_val_score(regr, df.iloc[:, :-1], df.iloc[:,-1:], cv = 10, scoring =
    'neg_mean_squared_error')).mean())


print('=== Random Forest ===')
regr = RandomForestRegressor(max_depth=2, random_state=0)
print('r2 = %.2f' % cross_val_score(regr, df.iloc[:, :-1],
    df.iloc[:,-1:] , cv = 10, scoring =
    'r2').mean())
print('rmse = %.2f' % np.sqrt(-1 * cross_val_score(regr, df.iloc[:, :-1],
    df.iloc[:,-1:], cv = 10, scoring =
    'neg_mean_squared_error')).mean())
regr = Pipeline([('trans', preprocessing.StandardScaler()), ('regr', regr)])
print('r2 = %.2f' % cross_val_score(regr, df.iloc[:, :-1],
    df.iloc[:,-1:] , cv = 10, scoring =
    'r2').mean())
print('rmse = %.2f' % np.sqrt(-1 * cross_val_score(regr, df.iloc[:, :-1], df.iloc[:,-1:], cv = 10, scoring =
    'neg_mean_squared_error')).mean())

'''
print('=== MLP ===')
regr = MLPRegressor()
print('r2 = %.2f' % cross_val_score(regr, df.iloc[:, :-1],
    df.iloc[:,-1:] , cv = 10, scoring =
    'r2').mean())
print('rmse = %.2f' % np.sqrt(-1 * cross_val_score(regr, df.iloc[:, :-1], df.iloc[:,-1:], cv = 10, scoring =
    'neg_mean_squared_error')).mean())
regr = Pipeline([('trans', preprocessing.StandardScaler()), ('regr', regr)])
print('r2 = %.2f' % cross_val_score(regr, df.iloc[:, :-1],
    df.iloc[:,-1:] , cv = 10, scoring =
    'r2').mean())
print('rmse = %.2f' % np.sqrt(-1 * cross_val_score(regr, df.iloc[:, :-1],
    df.iloc[:,-1:] , cv = 10, scoring =
    'neg_mean_squared_error')).mean())
'''

