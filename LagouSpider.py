# -*- coding:utf-8 -*-
# User: rudy
import urllib
import urllib2
import json
import MySQLdb
import pandas as pd
from pandas import Series,DataFrame
import re
from lxml import etree
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

conn=MySQLdb.connect(host="115.28.149.242",user="***",passwd="***",db="test",charset="utf8")
cursor = conn.cursor()


class LagouSpider(object):
    url = 'http://www.lagou.com/jobs/positionAjax.json'
    max_page = 1
    is_first = True
    rdata = None
    result = []

    def sendRequest(self, keyword, page_num=1):
        url = self.url+'?px=default'
        post_data = {}
        post_data['first'] = page_num == 1 and 'true' or 'false'
        post_data['kd'] = keyword
        post_data['pn'] = page_num
        post_data = urllib.urlencode(post_data)
        request = urllib2.urlopen(url,post_data)
        content = request.read()
        content = (json.loads(content))['content']
        self.max_page = content['totalPageCount']
        for item in content['result']:
            item['companyLabelList2'] = '-'.join(item['companyLabelList'])
            item.pop('companyLabelList')
            item.pop('adWord')
            item.pop('adjustScore')
            item.pop('calcScore')
            item.pop('companyLogo')
            item.pop('countAdjusted')
            item.pop('createTime')
            item.pop('haveDeliver')
            item.pop('hrScore')
            item.pop('positonTypesMap')
            item.pop('randomScore')
            item.pop('relScore')
            item.pop('score')
            item.pop('searchScore')
            item.pop('showOrder')
            item.pop('totalCount')
            item.pop('orderBy')
            item.pop('companyId')
            item.pop('createTimeSort')
            item.pop('leaderName')
            item.pop('positionType')
            item.pop('positionFirstType')
            e = Series(data=item).index
            if self.is_first:
                self.rdata = DataFrame(Series(data=item)).T
                self.is_first = False
            else:
                temp_rdata = DataFrame(Series(data=item)).T
                self.rdata = pd.concat([self.rdata,temp_rdata])
            self.result.append(item)
        pass

    def spiderByKeyword(self, keyword):
        page_num = 0
        while(page_num < self.max_page):
            page_num += 1
            self.sendRequest(keyword,page_num)

        self.rdata.index = range(1,len(self.rdata)+1)

        self.rdata['salarymin'] = 0
        self.rdata['salarymax'] = 0
        self.rdata['url'] = ''
        self.rdata['jd'] = ''#职位描述
        self.rdata['handle_perc'] = ''#简历及时处理率，在七天内处理完简历占所有简历的比例
        self.rdata['handle_day'] = ''#完成简历处理平均天数
        try:
            rdata = self.rdata
            tt = len(rdata['salary'])
            for klen in list(range(len(rdata['salary']))):
                rdata.ix[klen+1,'salarymin'] = re.search('^(\d*?)k',rdata['salary'].iloc[klen]).group(1)

                # 如果工资的最大值没有写，如（8k以上），则列为空值
                temp_salarymax = re.search('-(\d*?)k$', rdata['salary'].iloc[klen])
                if temp_salarymax:
                    rdata.ix[klen+1,'salarymax'] = temp_salarymax.group(1)
                else:
                    rdata.ix[klen+1,'salarymax'] = ''
                # 增加url一列，便于后续抓取jd内容
                rdata.ix[klen+1, 'url'] = 'http://www.lagou.com/jobs/%s.html' % rdata.ix[klen+1, 'positionId']

                # 对url进行二次抓取，把jd抓进来
                request = urllib2.urlopen(rdata.ix[klen+1, 'url'])
                data = request.read()
                html_tree = etree.HTML(data)
                temp_node = html_tree.xpath("//dd[@class='job_bt']/descendant::text()")
                rdata.ix[klen+1, 'jd'] = ''.join(temp_node).replace(' ', '')
                temp_node = html_tree.xpath("//span[@class='data']/text()")
                if re.search('(\d*)%', str(temp_node[0])):
                    rdata.ix[klen+1, 'handle_perc'] = re.search('(\d*)%', str(temp_node[0])).group(1)
                else:
                    rdata.ix[klen+1, 'handle_perc'] = ''
                rdata.ix[klen+1, 'handle_day'] = re.search('(\d*)', str(temp_node[1])).group(1)
        except Exception as e:
            print(e)

        # ew = pd.ExcelWriter('F:/py_www/lagou/test.xlsx',options={'encoding':'utf-8'})
        str_dict = self.rdata.to_dict()
        # ew.save()
        str_dict.pop('salary')
        str_dict.pop('url')
        dict_key = str_dict.keys()
        for klen in list(range(len(self.rdata['salary']))):
            temp = []
            for key in dict_key:
                temp.append(MySQLdb.escape_string(str(str_dict[key][klen+1])))
            s1 = ','.join(dict_key)
            s2 = '","'.join(temp)
            sql = 'insert lagou_source(%s) values("%s")' %(s1,s2)
            cursor.execute(sql)
            pass

if __name__ == '__main__':
    spider = LagouSpider()
    spider.spiderByKeyword('php工程师')
    pass

cursor.close()
conn.close()