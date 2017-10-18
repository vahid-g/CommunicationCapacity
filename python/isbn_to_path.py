import csv

with open('../data/amazon/isbn_new.csv', 'r') as csvfile:
    reader = csv.reader(csvfile)
    prefix = '/scratch/cluster-share/ghadakcv/data-sets/amazon/amazon-inex'
    for row in reader:
        isbn = row[0]
        dr = isbn[-3:]
        print('%s/%s/%s.xml,%s' % (prefix, dr, isbn, row[1]))

