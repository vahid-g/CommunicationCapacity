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
df[1] = df[1].cat.add_categories('UNK').fillna('UNK')
df[4] = df[4].str.split("[.']").str.get(0)
df[4] = df[4].astype('category')
df[4] = df[4].cat.add_categories('UNK_2').fillna('UNK_2')
cat_cols = df.select_dtypes(['category']).columns
df[cat_cols] = df[cat_cols].apply(lambda x: x.cat.codes)
print('column types: \n' + repr(df.dtypes))
df = df.fillna(df.mean())
print('data cleaning is done.')
print('data size' + repr(df.shape))
print('first row: \n' + repr(df.iloc[0, :]))

X = df.iloc[:,:-1]
y = df.iloc[:, -1:]


print('=== Gradient Boosting ===')
regr = make_pipeline(preprocessing.StandardScaler(), GradientBoostingRegressor(n_estimators=100, learning_rate=0.1,
        max_depth=1, random_state=0, loss='ls'))
'''
print('r2 = %.2f' % cross_val_score(regr, df.iloc[:, :-1],
    df.iloc[:,-1:], cv = 10, scoring =
    'r2').mean())
print('rmse = %.2f' % np.sqrt(-1 * cross_val_score(regr, df.iloc[:, :-1],
    df.iloc[:,-1:] , cv = 10, scoring =
    'neg_mean_squared_error')).mean())
'''
print('fitting model..')
clf = regr.fit(X, y)

print('reading features data..')
df = pd.read_csv('../data/amazon/everything/fixed.csv', header=None, low_memory=False)
print('cleaning data..')
isbns = df.as_matrix([0])
del(df[0])

df[1] = df[1].astype('category')
df[1] = df[1].cat.add_categories('UNK').fillna('UNK')
df[3] = df[3].mask(df[3] == 'summ', 0)
df[3] = df[3].astype('float32')
#df[3] = df[3].cat.add_categories('UNK_3').fillna('UNK_3')
df[4] = df[4].str.split("[.']").str.get(0)
df[4] = df[4].astype('category')
df[4] = df[4].cat.add_categories('UNK_2').fillna('UNK_2')
cat_cols = df.select_dtypes(['category']).columns
print('cat cols:' + repr(cat_cols))
df[cat_cols] = df[cat_cols].apply(lambda x: x.cat.codes)
df = df.fillna(df.mean())
print('pickling..')
df.to_pickle('clean.pk1')

print('df shape: ' + repr(df.shape))
pred = regr.predict(df)
pred =np.reshape(pred, (len(pred), 1))
print(pred.shape)
print(isbns.shape)
final = np.concatenate((isbns, pred), 1)
print(final.shape)
print(final[0])
np.savetxt('output.csv', final, fmt='%s, %.2f', delimiter=',')

