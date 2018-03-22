# helper functions for cache selection ml code 
from __future__ import division
import numpy as np
import pandas as pd
from sklearn.metrics import confusion_matrix

def print_results(y_test, y_pred):
    tn, fp, fn, tp = confusion_matrix(y_test, y_pred).ravel()
    print("precision = %f" % (tp / (tp + fp)))
    print("recall = %.2f" % (tp / (tp + fn)))
    print("negative predictive value= %.2f" % (tn / (tn + fn)))
    print("fallout = %.2f" % (tn / (tn + fp)))
    print("1s percentage = %.2f" % (100 * np.sum(y_pred) / y_pred.shape[0]))

