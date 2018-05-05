import csv
import sys

# /scratch/data-sets/wikipedia/textpath13_count13_title.csv
# /scratch/Workspace/Eclipse/database-capacity/data/wiki
with open(sys.argv[1], 'r') \
        as f1, \
        open(sys.argv[2], 'r') as f2, \
        open(sys.argv[3], 'w') as f3:
    c = csv.reader(f1, delimiter=',', quoting=csv.QUOTE_NONE)
    qrel_count = dict()
    for row in c:
        qrel = row[0].split('/')[-1].split('.')[0]
        qrel_count[qrel] = row[1]
    qrel_count['qrel'] = 'count'
    for line in f2:
        fields = line.split(',')
        qrel = fields[3]
        if qrel in qrel_count:
            count = qrel_count[qrel]
            f3.write(line.strip() + "," + count + '\n')
        else:
            print("qrel " + qrel +  " not in dict!")


