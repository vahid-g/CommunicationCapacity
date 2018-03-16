# runs simple classification on cache selection problem
import numpy as np
import pandas as pd
from sklearn.metrics import confusion_matrix

def print_results(y_test, y_pred):
    tn, fp, fn, tp = confusion_matrix(y_test, y_pred).ravel()
    print("precision = %.2f" % (tp / (tp + fp)))
    print("negative predictive value= %.2f" % (tn / (tn + fn)))
    # print("recall = %.2f" % (tp / (tp + fn)))
    print("fallout = %.2f" % (tn / (tn + fp)))
    print("1s percentage = %.2f \n" % (100 * np.sum(y_pred) / y_pred.shape[0]))

