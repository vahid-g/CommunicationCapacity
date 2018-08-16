import pandas as pd
import sys

def main(argv):
    f = argv[0]
    c = pd.read_csv('../../data/python_data/' + f)
    analyze(c, '18', '100', 'TestViewCount')

def analyze(c, subset, db, popularity):
    query_count = c.shape[0]
    print('distinct query count: %d' % query_count)
    if popularity not in c:
        print('warning: TestViewCount not found')
        c[popularity] = 1
    #c[popularity] = 1
    s = c[popularity].sum()
    #db_queries = (c['Pred'] * c[popularity]).sum()
    r1 = c[subset] * c[popularity]
    r2 = c[db] * c[popularity]
    r3 = c['ql'] * c[popularity]
    r4 = c['ml'] * c[popularity]
    r5 = c['best'] * c[popularity]
    r6 = c['rand'] * c[popularity]
    s = c[popularity].sum()
    print('set \t sub \t db \t ql \t ml \t best \t rand')
    print('all \t %.2f \t %.2f \t %.2f \t %.2f \t %.2f \t %.2f' %
          (r1.sum() / s, r2.sum() / s, r3.sum() / s, r4.sum() / s, r5.sum() /
           s, r6.sum() / s))
    b = c['Label'] == 1
    s = c[popularity][b].sum()
    r1 = c[subset][b] * c[popularity][b]
    r2 = c[db][b] * c[popularity][b]
    r3 = c['ql'][b] * c[popularity][b]
    r4 = c['ml'][b] * c[popularity][b]
    r5 = c['best'][b] * c[popularity][b]
    r6 = c['rand'][b] * c[popularity][b]
    sdf= c[popularity][b].sum()
    print('bad \t %.2f \t %.2f \t %.2f \t %.2f \t %.2f \t %.2f' %
          (r1.sum() / s, r2.sum() / s, r3.sum() / s, r4.sum() / s, r5.sum() /
           s, r6.sum() / s))
    bad_count = b.value_counts()[True]
    print('%d distinct bad queries (%.2f %%)' %
          (bad_count, bad_count * 100 / query_count))

if __name__ == "__main__":
    main(sys.argv[1:])

