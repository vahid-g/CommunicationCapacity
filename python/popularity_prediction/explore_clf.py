import math
import main
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn import linear_model
from sklearn import preprocessing
from sklearn import tree
from sklearn import ensemble
from sklearn.pipeline import Pipeline, make_pipeline
from sklearn.preprocessing import PolynomialFeatures
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import cross_val_score
from sklearn.metrics import mean_squared_error, r2_score, make_scorer
from sklearn.naive_bayes import GaussianNB
from sklearn.neighbors.nearest_centroid import NearestCentroid
from sklearn.neural_network import MLPRegressor
from sklearn import svm

df, _ = main.prep_data('../data/amazon/ml_files/bigtrainset_bin.csv')
print(df.describe())
X = df.iloc[:,:-1]
y = df.iloc[:,-1]
clfs = list()
clfs.append(linear_model.LogisticRegression())
#clfs.append(tree.DecisionTreeClassifier(max_depth=None, min_samples_split=2, random_state=0))
clfs.append(ensemble.RandomForestClassifier(n_estimators=100,
                max_depth=None, min_samples_split=2, random_state=0))
#clfs.append(linear_model.SGDClassifier(loss="hinge", penalty="l2"))
#clfs.append(NearestCentroid(metric='euclidean', shrink_threshold=None))

for clf in clfs:
    print('executing ' + repr(clf))
    pip = make_pipeline(preprocessing.StandardScaler(),
            clf)
    print('pre = %.2f' % cross_val_score(pip, X, y, cv = 10, scoring = 'precision').mean())
    print('rec = %.2f' % cross_val_score(pip, X, y, cv = 10, scoring = 'recall').mean())

