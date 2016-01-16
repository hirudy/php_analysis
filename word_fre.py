# -*- coding:utf-8 -*-
# User: rudy
# Time: 2015/11/01

import MySQLdb
import jieba
import pandas as pd
from pandas import Series,DataFrame

conn=MySQLdb.connect(host="115.28.149.242",user="***",passwd="***",db="test",charset="utf8")
cursor = conn.cursor()

jieba.load_userdict('./foobar.txt')
jieba.del_word('web')
sql = 'SELECT jd FROM lagou_source'
cursor.execute(sql)
result = cursor.fetchall()

ci_arr = {}
for item in result:
    temp_arr = jieba.lcut(item[0])
    for key in temp_arr:
        if len(key) >= 3:
            temp = key.lower()
            if temp in ci_arr:
                ci_arr[temp] += 1
            else:
                ci_arr[temp] = 1


is_first = True
rdata = None
t = []
for item in ci_arr.items():
    if item[1] >= 2:
        t.append(item)

t.sort(lambda a, b: cmp(a[1], b[1]),reverse=True)

for item in t:
    if is_first:
        rdata = DataFrame(Series(data=item)).T
        is_first = False
    else:
        temp_rdata = DataFrame(Series(data=item)).T
        rdata = pd.concat([rdata, temp_rdata])
ew = pd.ExcelWriter('F:/py_www/lagou/word_fre.xlsx',options={'encoding':'utf-8'})
str_dict = rdata.to_excel(ew)
ew.save()
cursor.close()
conn.close()