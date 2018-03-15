# runs simple classification on cache selection problem
from __future__ import division
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.model_selection import GridSearchCV
from sklearn.preprocessing import StandardScaler
from sklearn import linear_model
from sklearn.metrics import confusion_matrix
from sklearn import svm
from sklearn.ensemble import RandomForestClassifier

def print_results(y_test, y_pred):
    tn, fp, fn, tp = confusion_matrix(y_test, y_pred).ravel()
    print("precision = %.2f" % (tp / (tp + fp)))
    print("negative predictive value= %.2f" % (tn / (tn + fn)))
    #print("recall = %.2f" % (tp / (tp + fn)))
    print("fallout = %.2f" % (tn / (tn + fp)))
    #print("tp = %d" % tp)
    print("1s percentage = %.2f \n" % (100 * np.sum(y_pred) / y_pred.shape[0]))

df = pd.read_csv('../../data/python_data/cache_pred_new.csv')
df = df.fillna(0)
df = df.drop(['Bm_25', 'min_bm25', 'bool_score', 'min_bool_score', 'Bm_25_sub',
'min_bm25_sub', 'bool_score_sub', 'min_bool_score_sub'], axis=1)
df_size = df.shape[0]
train = df.sample(frac=0.66, random_state=1)
train = train.drop(['query'], axis=1)
test = df.loc[~df.index.isin(train.index)]
test_queries = test['query']
test = test.drop(['query'], axis=1)
cols = train.columns.tolist()
# print(df.corr()['label'].sort_values())

# learn the model
X = train[cols[:-1]]
y = train[cols[-1]]
print("1s percentage in training = %.2f \n" % (100 * np.sum(y) / y.shape[0]))
sc = StandardScaler().fit(X)
X = sc.transform(X)
lr = linear_model.LogisticRegression()
lr.fit(X, y)
print("training mean accuracy = %.2f" % lr.score(X, y))

''' parameter tuning
tuned_parameters = [{'C': [0.01, 0.1, 1, 10, 100, 1000]}]
scores = ['precision', 'recall']
for score in scores:
    clf = GridSearchCV(linear_model.LogisticRegression(), tuned_parameters, cv=5,
            scoring='%s_macro' % score)
    clf.fit(X, y)
    print('best params:')
    print(clf.best_params_)
'''

X_test = test[cols[:-1]]
y_test = test[cols[-1]]
X_test = sc.transform(X_test)
print("testing mean accuracy = %.2f" % lr.score(X_test, y_test))
y_pred = lr.predict(X_test)
print_results(y_test, y_pred)

print("balanced learning")
lr = linear_model.LogisticRegression(class_weight='balanced')
lr.fit(X, y)
print("training mean accuracy = %.2f" % lr.score(X, y))
print("testing mean accuracy = %.2f" % lr.score(X_test, y_test))
# print('coefs:')
# coef = np.sort(lr.coef_.flatten())
# print(np.column_stack((train.columns.values[:-1], coef)))
y_pred = lr.predict(X_test)
y_prob = lr.predict_proba(X_test)
y_pred = y_prob[:, 1] > 0.5
print("threshold learning 0.5")
print_results(y_test, y_pred)
print("threshold learning 0.7")
y_pred = y_prob[:, 1] > 0.7
y_pred = y_pred.astype('uint8')
print_results(y_test, y_pred)
print("threshold learning 0.8")
y_pred = y_prob[:, 1] > 0.8
y_pred = y_pred.astype('uint8')
print_results(y_test, y_pred)

print("random forest..")
clf = RandomForestClassifier(n_estimators=10, class_weight='balanced')
clf.fit(X,y)
print("training mean accuracy = %.2f" % lr.score(X, y))
y_pred = clf.predict(X_test)
print_results(y_test, y_pred)

print("random forest with n = 50 ")
clf = RandomForestClassifier(n_estimators=50, class_weight='balanced')
clf.fit(X,y)
print("training mean accuracy = %.2f" % lr.score(X, y))
y_pred = clf.predict(X_test)
print_results(y_test, y_pred)


print("unbalanced random forest..")
clf = RandomForestClassifier(n_estimators=50)
clf.fit(X,y)
print("training mean accuracy = %.2f" % lr.score(X, y))
y_pred = clf.predict(X_test)
print_results(y_test, y_pred)

'''
print("svc results..")
clf = svm.SVC(kernel='linear', class_weight='balanced')
clf.fit(X, y)
y_pred = clf.predict(X_test)
print_results(y_test, y_pred)
'''
'''
output = pd.DataFrame()
output['query'] = test_queries
output['pred'] = pd.Series(y_pred, index=output.index)
output.to_csv('../../data/python_data/ml_result.csv')
'''
