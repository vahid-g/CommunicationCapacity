import pandas as pd

a = pd.read_csv('../../data/python_data/stack.csv')
b = pd.read_csv('../../data/python_data/labels.csv')
b = b.drop(['15','100','label'], axis=1)
c = a.merge(b, on='query')
c.to_csv('../../data/python_data/stack_train.csv', index=False)
