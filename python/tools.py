import pandas as pd
import nampy as np

# this function computes the p-value of two series with their frequency
def my_ttest(f, x, y):
m1 = (x * f).sum() / n
m2 = (y * f).sum() / n
v1 = ((x - m1).pow(2) * f).sum() / n
v2 = ((y - m2).pow(2) * f).sum() / n
s = np.sqrt((v1 + v2) / 2)
df = 2*n - 2
t = (m1 - m2)/(s * np.sqrt(2/n))
p = 1 - stats.t.cdf(t, df=df)

