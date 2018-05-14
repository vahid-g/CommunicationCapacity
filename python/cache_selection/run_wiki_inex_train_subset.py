''' runs simple classification on cache selection problem '''
from __future__ import division
import sys
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import MinMaxScaler
from sklearn import linear_model
from sklearn.model_selection import RepeatedKFold
from sklearn.model_selection import KFold
from cache_pred import print_results

def main(argv):
    filename = argv[0]
    split = 5
    df = pd.read_csv('../../data/python_data/' + filename)
    df = df.drop(['query'], axis = 1)
    skf = RepeatedKFold(n_splits=split, random_state = 1, n_repeats = 100)
    mean = np.zeros([50, 1])
    for train_index, test_index in skf.split(df):
        X_train, X_test = df.iloc[train_index], df.iloc[test_index]
        mean += X_train.mean()[:, np.newaxis]
        sys.stdout.write('%s ' % X_train.mean().argmax())
    print
    print(np.argmax(mean))

if __name__ == "__main__":
    main(sys.argv[1:])
