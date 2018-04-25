
import csv

with open('stack.csv', 'r') as fr, open('stack.out', 'w') as fw:
    reader = csv.reader(fr, delimiter=',', quoting=csv.QUOTE_NONE)
    writer = csv.writer(fw, delimiter=',', quotechar='"',
                        quoting=csv.QUOTE_MINIMAL)
    for row in reader:
        l = len(row)
        #print(l)
        if (l == 37):
            writer.writerow(row)
        elif(l > 37):
            m = l - 36
            query = ' '.join(row[:m])
            writer.writerow([query] + row[m:])
        else:
            print(','.join(row))
