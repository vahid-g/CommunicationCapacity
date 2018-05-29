import pandas as pd
import sys

def main(argv):
    f = argv[0]
    c = pd.read_csv('../../data/python_data/' + f, '18', '100',
                    'TestViewCount')

def analyze(c, subset, db, popularity):
    zero_count = c['ml'].value_counts()[0]
    query_count = c.shape[0]
    print('distinct query count: %d' % query_count)
    print('%d queries submitted to the subset (%.2f %%)' %
          (zero_count, zero_count * 100 / query_count))
    print('average mrr:')
    if popularity not in c:
        print('warning: TestViewCount not found')
        c[popularity] = 1
    #c[popularity] = 1
    s = c[popularity].sum()
    r1 = c[subset] * c[popularity]
    r2 = c[db] * c[popularity]
    r3 = c['ql'] * c[popularity]
    r4 = c['ml'] * c[popularity]
    r5 = c['best'] * c[popularity]
    s = c[popularity].sum()
    print('sub \t db \t ql \t ml \t best')
    print('%.2f \t %.2f \t %.2f \t %.2f \t %.2f' %
          (r1.sum() / s, r2.sum() / s, r3.sum() / s, r4.sum() / s, r5.sum() / s))
    b = c[subset] < c[db]
    bad_count = b.value_counts()[True]
    print('%d distinct bad queries (%.2f %%)' %
          (bad_count, bad_count * 100 / query_count))
    print('average mrr for bad queries:')
    b = c['Label'] == 1
    s = c[popularity][b].sum()
    r1 = c[subset][b] * c[popularity][b]
    r2 = c[db][b] * c[popularity][b]
    r3 = c['ql'][b] * c[popularity][b]
    r4 = c['ml'][b] * c[popularity][b]
    r5 = c['best'][b] * c[popularity][b]
    sdf= c[popularity][b].sum()
    print('sub \t db \t ql \t ml \t best')
    print('%.2f \t %.2f \t %.2f \t %.2f \t %.2f' %
          (r1.sum() / s, r2.sum() / s, r3.sum() / s, r4.sum() / s, r5.sum() / s))

if __name__ == "__main__":
    main(sys.argv[1:])

