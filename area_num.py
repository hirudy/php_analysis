# -*- coding:utf-8 -*-
# User: rudy
# Time: 2015/11/01

import MySQLdb
import pandas as pd
from pandas import Series,DataFrame

conn=MySQLdb.connect(host="115.28.149.242",user="***",passwd="***",db="test",charset="utf8")
cursor = conn.cursor()

sql = 'SELECT city,COUNT(*) AS num FROM lagou_source GROUP BY city ORDER BY num DESC'
cursor.execute(sql)
result = cursor.fetchall()
is_first = True
rdata = None
for item in result:
    if is_first:
        rdata = DataFrame(Series(data=item)).T
        is_first = False
    else:
        temp_rdata = DataFrame(Series(data=item)).T
        rdata = pd.concat([rdata, temp_rdata])
ew = pd.ExcelWriter('F:/py_www/lagou/area_num.xlsx',options={'encoding':'utf-8'})
str_dict = rdata.to_excel(ew)
ew.save()
cursor.close()
conn.close()
