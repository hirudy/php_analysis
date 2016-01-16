# -*- coding:utf-8 -*-
# User: rudy
# Time: 2015/11/01

import MySQLdb
import pandas as pd
from pandas import Series,DataFrame

conn=MySQLdb.connect(host="115.28.149.242",user="***",passwd="***",db="test",charset="utf8")
cursor = conn.cursor()

sql = 'SELECT workYear,salarymin,salarymax FROM lagou_source'
cursor.execute(sql)
result = cursor.fetchall()
is_first = True
rdata = None
t = {}
t[u'应届毕业生'] = None
t[u'1年以下'] = None
t[u'1-3年'] = None
t[u'3-5年'] = None
t[u'不限'] = None
for item in result:
    temp = {}
    temp[0] = item[0]
    temp[1] = 1
    temp[2] = 1
    temp[3] = 1
    temp[4] = 1
    temp[5] = 1
    temp[6] = 1
    if t[item[0]]:
        temp_2 = item[2] == '' and 30 or item[2]
        temp_num = int((int(item[1])+int(temp_2))/2)
        if temp_num <5:
            t[item[0]][1] += 1
        elif temp_num < 10:
            t[item[0]][2] += 1
        elif temp_num < 15:
            t[item[0]][3] += 1
        elif temp_num < 20:
            t[item[0]][4] += 1
        elif temp_num < 25:
            t[item[0]][5] += 1
        else:
            t[item[0]][6] += 1
    else:
        t[item[0]] = temp
pass
for key,item in t.items():
    if is_first:
        rdata = DataFrame(Series(data=item)).T
        is_first = False
    else:
        temp_rdata = DataFrame(Series(data=item)).T
        rdata = pd.concat([rdata, temp_rdata])
ew = pd.ExcelWriter('F:/py_www/lagou/exp_pay.xlsx',options={'encoding':'utf-8'})
str_dict = rdata.to_excel(ew)
ew.save()
cursor.close()
conn.close()
