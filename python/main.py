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
from sklearn.metrics import average_precision_score, accuracy_score
from sklearn.naive_bayes import GaussianNB
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.neural_network import MLPRegressor

def prep_data(path):
    df = pd.read_csv(path, header=None, low_memory=False)
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
    df[3] = df[3].astype('category')
    df[3] = df[3].mask(df[3] == 'summ', 0)
    df[3] = df[3].astype('float32')
    df[4] = df[4].str.split("[.']").str.get(0)
    df[4] = df[4].astype('category')
    df[4] = df[4].cat.add_categories('UNK_2').fillna('UNK_2')
    cat_cols = df.select_dtypes(['category']).columns
    print('cat cols:' + repr(cat_cols))
    df[cat_cols] = df[cat_cols].apply(lambda x: x.cat.codes)
    print('column types: \n' + repr(df.dtypes))
    df = df.fillna(df.mean())
    print('data cleaning is done.')
    print('data size' + repr(df.shape))
    print('first row: \n' + repr(df.iloc[0, :]))
    return df, isbns


def main():
    print('training..')
    df, isbns = prep_data("../data/amazon/ml_files/trainset_bin.csv");
    X = df.iloc[:,:-1]
    y = df.iloc[:, -1]

    regr = make_pipeline(preprocessing.StandardScaler(),
            linear_model.LogisticRegression(class_weight = {0: 0.9, 1:0.1}))
    print('acc = %.2f' % cross_val_score(regr, X, y, cv = 10, scoring = 'accuracy').mean())
    print('fitting model..')
    clf = regr.fit(X, y)

    print('testing..')
    df, isbns = prep_data('../data/amazon/ml_files/testset_bin.csv')
    X = df.iloc[:,:-1]
    y = df.iloc[:, -1:]
    pred = clf.predict(X)
    acc = accuracy_score(y, pred)
    print('ACC: %.4f' % acc)
    ap = average_precision_score(y, pred)
    print('AP: %.4f' % ap)

    print('predicting for all data..')
    df, isbns = prep_data('../data/amazon/ml_files/features.csv')
    #df.to_pickle('clean.pk1')
    # pred = regr.predict(df)
    prob = regr.predict_proba(df)
    pred = prob[:,1] > 0.9
    pred =np.reshape(pred, (len(pred), 1))
    print(pred.shape)
    print(isbns.shape)
    final = np.concatenate((isbns, pred), 1)
    print(final.shape)
    print(final[0])
    np.savetxt('../data/python_data/output2.csv', final, fmt='%s,%s', delimiter=',')

if __name__ == '__main__':
    main()
