from __future__ import division
import math
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import sklearn.metrics as metrics
import pickle
from sklearn import ensemble
from sklearn import linear_model
from sklearn import preprocessing
from sklearn import tree
from sklearn import decomposition
from sklearn import neighbors
from sklearn.pipeline import Pipeline, make_pipeline
from sklearn.preprocessing import PolynomialFeatures
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import cross_val_score
from sklearn.metrics import mean_squared_error, r2_score, make_scorer
from sklearn.naive_bayes import GaussianNB
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.neural_network import MLPRegressor

def prep_data(path):
    df = pd.read_csv(path, header=None, low_memory=False)
    # print(df.describe())
    #print('first row: \n' + repr(df.iloc[0, :]))
    #print('column types: \n' + repr(df.dtypes))
    print('cleaning data..')
    print('df size: ' + repr(df.shape))
    isbns = df.as_matrix([0])
    del(df[0]) # removing isbns
    del(df[4]) # removing dewey category
    del(df[7]) # removing redundant feature (number of reviews)
    df[1] = df[1].astype('category')
    df[1] = df[1].cat.add_categories('UNK').fillna('UNK')
    df[3] = df[3].astype('category')
    df[3] = df[3].mask(df[3] == 'summ', 0)
    df[3] = df[3].astype('float32')
    '''
    df[4] = df[4].str.split("[.']").str.get(0)
    df[4] = df[4].astype('category')
    df[4] = df[4].cat.add_categories('UNK_2').fillna('UNK_2')
    '''
    cat_cols = df.select_dtypes(['category']).columns
    # print('cat cols:' + repr(cat_cols))
    # df[cat_cols] = df[cat_cols].apply(lambda x: x.cat.codes)
    # df = pd.get_dummies(df, prefix=['pr1', 'pr2'])
    # print('df1 description..')
    # print(df[1].describe())
    df1 = pd.get_dummies(df[1])
    pca = decomposition.PCA(n_components = 5).fit(df1)
    df1 = pca.transform(df1)
    del(df[1])
    df.append(pd.DataFrame(df1))
    '''
    print('describe: ' + repr(df[4].describe()))
    df4 = pd.get_dummies(df[4])
    pca = decomposition.PCA(n_components = 5).fit(df4)
    df4 = pca.transform(df4)
    del(df[4])
    df.append(pd.DataFrame(df4))
    '''
    df = df.fillna(df.mean())
    print('data cleaning is done.')
    print('df size: ' + repr(df.shape))
    return df, isbns


def main():
    print('training..')
    df, isbns = prep_data("../data/amazon/ml_files/bigtrainset_bin.csv");
    X = df.iloc[:,:-1]
    y = df.iloc[:, -1]
    print('onez in y: %d' % np.count_nonzero(y))
    print('y size: ' + repr(y.size))
    print(np.count_nonzero(y)/y.size)
    y = y.astype('int32')
    scaler = preprocessing.StandardScaler().fit(X)
    X = scaler.transform(X)
    logreg = linear_model.LogisticRegression(max_iter = 10000, verbose =
            0,class_weight = {0: 0.99, 1:0.01}, solver = 'lbfgs').fit(X, y)
    rf = ensemble.RandomForestClassifier(n_estimators=100, max_depth=None, min_samples_split=2,
            # random_state = 0)
            random_state=0, class_weight = {0:1, 1:6})
    knn = neighbors.KNeighborsClassifier(weights='uniform', n_neighbors=100)
    regr = make_pipeline(preprocessing.StandardScaler(), knn)
            #linear_model.LogisticRegression(class_weight = {0: 0.99, 1:0.01}))
    # print('pre = %.2f' % cross_val_score(regr, X, y, cv = 10, scoring = 'precision').mean())
    # print('rec = %.2f' % cross_val_score(regr, X, y, cv = 10, scoring = 'recall').mean())
    # print('params: ' + repr(regr.get_params()))
    # print('coef: ' + repr(logreg.coef_))

    print('fitting model..')
    clf = regr.fit(X, y)
    # pickle.dump(clf, open('model', 'wb'))
    print('testing..')
    df, isbns = prep_data('../data/amazon/ml_files/bigtestset_bin.csv')
    X = df.iloc[:, :-1]
    y = df.iloc[:, -1:]
    print('onez in y: %d' % np.count_nonzero(y))
    print('y size: ' + repr(y.size))
    print(np.count_nonzero(y)/y.size)
    pred = clf.predict(X)
    print('testing with thresh = .5')
    print('pre: %.4f' % metrics.precision_score(y, pred))
    print('rec: %.4f' % metrics.recall_score(y, pred))
    print('testing with thresh > .5')
    prob = clf.predict_proba(X)
    pred = prob[:,1] > 0.75
    print('pre: %.4f' % metrics.precision_score(y, pred))
    print('rec: %.4f' % metrics.recall_score(y, pred))
    print('onez in pred: %d' % np.count_nonzero(pred))
    print('onez in y: %d' % np.count_nonzero(y))

    '''
    print('predicting for all data..')
    df, isbns = prep_data('../data/amazon/ml_files/features.csv')
    #df.to_pickle('clean.pk1')
    pred = regr.predict(df)
    print('onez: %d' % np.count_nonzero(pred))
    #prob = regr.predict_proba(df)
    #pred = prob[:,1] > 0.9
    pred =np.reshape(pred, (len(pred), 1))
    final = np.concatenate((isbns, pred), 1)
    np.savetxt('../data/python_data/output3.csv', final, fmt='%s,%s', delimiter=',')
    '''

if __name__ == '__main__':
    main()
