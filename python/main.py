import math
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import sklearn.metrics as metrics
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

def prep_data(path):
    df = pd.read_csv(path, header=None, low_memory=False)
    # print(df.describe())
    print('data size: ' + repr(df.shape))
    #print('first row: \n' + repr(df.iloc[0, :]))
    #print('column types: \n' + repr(df.dtypes))
    print('cleaning data..')
    print('removing IDs..')
    isbns = df.as_matrix([0])
    del(df[0])
    print('converting categorical values to numerical values')
    df[1] = df[1].astype('category')
    df[1] = df[1].cat.add_categories('UNK').fillna('UNK')
    df[3] = df[3].astype('category')
    df[3] = df[3].mask(df[3] == 'summ', 0)
    df[3] = df[3].astype('float32')
    df[4] = df[4].str.split("[.']").str.get(0)
    df[4] = df[4].astype('category')
    df[4] = df[4].cat.add_categories('UNK_2').fillna('UNK_2')
    cat_cols = df.select_dtypes(['category']).columns
    print('cat cols:' + repr(cat_cols))
    df[cat_cols] = df[cat_cols].apply(lambda x: x.cat.codes)
    df = df.fillna(df.mean())
    print('data cleaning is done.')
    return df, isbns


def main():
    print('training..')
    df, isbns = prep_data("../data/amazon/ml_files/bigtrainset_bin.csv");
    X = df.iloc[:,:-1]
    y = df.iloc[:, -1]
    print (y.dtypes)
    y = y.astype('int32')

    regr = make_pipeline(preprocessing.StandardScaler(),
            #linear_model.LogisticRegression(class_weight = {0: 0.99, 1:0.01}))
            linear_model.LogisticRegression())
    print('acc = %.2f' % cross_val_score(regr, X, y, cv = 10, scoring = 'accuracy').mean())
    print('pre = %.2f' % cross_val_score(regr, X, y, cv = 10, scoring = 'precision').mean())
    print('rec = %.2f' % cross_val_score(regr, X, y, cv = 10, scoring = 'recall').mean())
    print('fitting model..')
    clf = regr.fit(X, y)
    print('testing..')
    df, isbns = prep_data('../data/amazon/ml_files/bigtestset_bin.csv')
    X = df.iloc[:,:-1]
    y = df.iloc[:, -1:]
    print('testing with thresh = .5')
    pred = clf.predict(X)
    print('pre: %.4f' % metrics.precision_score(y, pred))
    print('rec: %.4f' % metrics.recall_score(y, pred))
    print('testing with thresh > .5')
    prob = clf.predict_proba(X)
    pred = prob[:,1] > 0.9
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
