import math
import main
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
df, _ = prep_data('')
print(df.describe())

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

