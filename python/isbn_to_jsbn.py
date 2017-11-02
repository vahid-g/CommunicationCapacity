# does data matching between files in INEX dataset and meta-data in julians
# files. Uses the 'title' field to match the books and their meta-data

import csv
term_jsbn = dict()
plus_jsbn = dict()
with open('../data/amazon/julian_anal/jsbn_title', 'r') as jfile:
    # more data cleaning ideas:
    #   (1) remove all colons
    #   (2) replaec &amp; with and
    reader = csv.reader(jfile, delimiter=' ')
    for rows in reader:
        title = rows[1].strip('" .').lower()
        term_jsbn[title] = rows[0]
        for ch in [':', '(']:
            if ch in title:
                token = title[:title.find(ch)].strip()
                if token not in plus_jsbn:
                    plus_jsbn[token] = list()
                plus_jsbn[token].append(rows[0])
                break;
jfile.close()
print(len(term_jsbn))
with open('../data/amazon/from_inex/isbn_title.csv', 'r') as csvfile, open('../data/amazon/isbn_jsbn', 'w') as output:
        reader = csv.reader(csvfile)
        for rows in reader:
            found = False
            title = rows[1].strip('" .').lower()
            if title in term_jsbn:
                output.write(rows[0] + ' ' + term_jsbn[title] + '\n')
            elif title in plus_jsbn:
                for i in plus_jsbn[title]:
                    output.write(rows[0] + ' ' + i + '\n')
            # note that followin line just find one match out of many
            elif (':' in title) or ('(' in title):
                for ch in [':', '(']:
                    if ch in title:
                        token = title[:title.find(ch)].strip()
                        if token in plus_jsbn:
                            for i in plus_jsbn[token]:
                                output.write(rows[0] + ' ' + i + '\n')
            else:
                print(rows[1] + " " + repr(rows[1] in term_jsbn) + " " +
                    repr(rows[1] in plus_jsbn))

csvfile.close()
output.close()
