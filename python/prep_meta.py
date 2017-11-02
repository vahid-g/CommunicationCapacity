# generates jsbn_title and jsbn_srank files based on julian's meta_Books json
# file.

with open('../data/amazon/julian_raw/meta_Books.json', 'r') as json, \
    open('../data/amazon/julian_anal/jsbn_title', 'w') as out1, \
        open('../data/amazon/julian_anal/jsbn_srankrev', 'w') as out2:
    for l in json:
        dic = eval(l)
        # note! if an item has more than one salesrank we pick the first one
        # furthermore we reverse the salesrank
        salesrank = "-1"
        if 'salesRank' in dic:
            rank_dic = dic['salesRank']
            salesrank = str(15000000 - rank_dic[rank_dic.keys()[0]])
        title  = ""
        if 'title' in dic:
            title = dic['title']
        out1.write(dic['asin'] + ' "' + title + '"\n')
        out2.write(dic['asin'] + ' ' + salesrank + '\n')
json.close()
out1.close()
out2.close()
