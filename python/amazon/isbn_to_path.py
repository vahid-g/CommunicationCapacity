# converts the isbn->weight file to path->weight
import csv
with open('../data/amazon/isbn_all', 'r') as csvfile, \
    open('../data/amazon/amazon_path_ratecount.csv', 'w') as output:
    reader = csv.reader(csvfile, delimiter=' ')
    prefix = '/scratch/cluster-share/ghadakcv/data-sets/amazon/amazon-inex'
    for row in reader:
        if len(row) < 2:
            break
        isbn = row[0]
        dr = isbn[-3:]
        output.write('%s/%s/%s.xml,%s\n' % (prefix, dr, isbn, row[1]))
csvfile.close()
output.close()
