import pandas as pd

a = pd.read_csv('../../data/python_data/stack_result.csv')
a = a.drop('label', axis=1)
b = pd.read_csv('../../data/python_data/labels.csv')
c = a.merge(b, on='query')
def f(row):
    if row['pred'] == 1:
        val = row['100']
    else:
        val = row['15']
    return val
def g(row):
    if row['label'] == 1:
        val = row['100']
    else:
        val = row['15']
    return val



c['ml'] = c.apply(f, axis=1)
c['best'] = c.apply(g, axis=1)

c.to_csv('../../data/python_data/stack_final.csv', index=False)
